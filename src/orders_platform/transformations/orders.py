from pyspark.sql import (
    functions as F
)
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DecimalType
)


def parse_orders_cdc(cdc_df):
    cdc_df = cdc_df.filter(F.col('topic') == 'shop.public.orders')
    if cdc_df.filter(
            F.col('partition').isNull() | (F.col('partition') != 0)
    ).limit(1).count() > 0:
        raise ValueError('Partition invalid')

    if cdc_df.filter(
            (F.col('offset') < 0) | F.col('offset').isNull()
    ).limit(1).count()>0:
        raise ValueError('Offset invalid')

    key_schema = StructType([
        StructField("order_id", LongType(), True),
    ])
    cdc_df = cdc_df.withColumn('parsed_key', F.from_json(F.col('key'), key_schema))

    order_schema = StructType([
        StructField("order_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("status", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("shipping_amount", StringType(), True),
        StructField("delivery_region", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])

    source_schema = StructType([
        StructField('ts_ms', LongType(), True)
    ])

    value_schema = StructType([
        StructField("source", source_schema, True),
        StructField("op", StringType(), True),
        StructField("after", order_schema, True),
    ])

    cdc_df = cdc_df.filter(F.col('value').isNotNull())
    cdc_df = cdc_df.withColumn('parsed_value', F.from_json(F.col('value'), value_schema))

    invalid_partitions = (
        cdc_df
        .groupBy('parsed_key.order_id')
        .agg(
            F.countDistinct('partition').alias('partition_cnt')
        )
        .filter(F.col('partition_cnt') > 1)
    )
    if invalid_partitions.limit(1).count() > 0:
        invalid_partitions.show(20, truncate=False)
        raise ValueError(
            'One order ID events in different partitions'
        )

    if cdc_df.filter(F.col('parsed_key.order_id').isNull()).limit(1).count() > 0:
        raise ValueError(
            'Data contains null keys'
        )

    if cdc_df.filter(
            (F.col('parsed_value.op').isNull()) | (~F.col('parsed_value.op').isin(['c', 'r', 'u', 'd']))
    ).limit(1).count() > 0:
        raise ValueError(
            'Invalid table operation in data'
        )

    cru_operations = cdc_df.filter(F.col('parsed_value.op').isin(['c', 'r', 'u']))
    if cru_operations.filter(F.col('parsed_value.after').isNull()).limit(1).count() > 0:
        raise ValueError(
            'Not after values for CRU operations'
        )
    if cru_operations.filter(
            (F.col('parsed_value.after.order_id').isNull()) | (
                    F.col('parsed_key.order_id') != F.col('parsed_value.after.order_id'))
    ).limit(1).count() > 0:
        raise ValueError(
            'Order ID value in key does not match value'
        )

    if cdc_df.filter(
            F.col('parsed_value.op').isin(['c', 'r', 'u']) & F.to_timestamp('parsed_value.after.created_at').isNull()
    ).limit(1).count() > 0:
        raise ValueError(
            'Created at value in null'
        )

    if cdc_df.filter(
            F.col('parsed_value.op').isin(['c', 'r', 'u']) & F.to_timestamp('parsed_value.after.updated_at').isNull()
    ).limit(1).count() > 0:
        raise ValueError(
            'Updated at value in null'
        )

    cdc_df = cdc_df.withColumn(
        "shipping_amount_decimal",
        F.col("parsed_value.after.shipping_amount").cast(DecimalType(18, 2))
    )
    if cdc_df.filter(
        F.col('parsed_value.op').isin(['c', 'r', 'u'])
        & (F.col('shipping_amount_decimal').isNull() | (F.col('shipping_amount_decimal') < 0))
    ).limit(1).count() > 0:
        raise ValueError('Shipping amount value in null')

    if cdc_df.filter(
        F.col('parsed_value.op').isin(['c', 'r', 'u'])
        & (F.col('parsed_value.after.customer_id').isNull())
    ).limit(1).count() > 0:
        raise ValueError('Customer ID is null')

    if cdc_df.filter(
        F.col('parsed_value.op').isin(['c', 'r', 'u'])
        & (
            F.col('parsed_value.after.currency').isNull()
        )
    ).limit(1).count() > 0:
        raise ValueError('Currency is null')

    if cdc_df.filter(
        F.col('parsed_value.op').isin(['c', 'r', 'u'])
        & (
            F.col('parsed_value.after.delivery_region').isNull()
        )
    ).limit(1).count() > 0:
        raise ValueError('Delivery region is null')

    if cdc_df.filter(
        F.col('parsed_value.op').isin(['c', 'r', 'u'])
        & (
            F.col('parsed_value.after.status').isNull()
            | (~F.col('parsed_value.after.status').isin('created', 'paid', 'shipped', 'delivered', 'cancelled'))
        )
    ).limit(1).count() > 0:
        raise ValueError('Status is null')

    cdc_df = cdc_df.select(
        F.col('parsed_key.order_id').alias('order_id'),
        F.col('parsed_value.after.customer_id').alias('customer_id'),
        F.col('parsed_value.after.status').alias('status'),
        F.col('parsed_value.after.currency').alias('currency'),
        F.col('shipping_amount_decimal').alias('shipping_amount'),
        F.col('parsed_value.after.delivery_region').alias('delivery_region'),
        F.col('parsed_value.after.created_at').alias('created_at'),
        F.col('parsed_value.after.updated_at').alias('updated_at'),
        F.col('parsed_value.op').alias('operation'),
        F.col('parsed_value.source.ts_ms').alias('source_changed_at'),
        F.col('topic').alias('topic'),
        F.col('partition').alias('partition'),
        F.col('offset').alias('offset'),
        F.col('ingested_at').alias('ingested_at')
    )

    cdc_df = cdc_df.withColumns({
        c: F.to_timestamp(c) for c in ['created_at', 'updated_at']
    })
    cdc_df = cdc_df.withColumn(
        'source_changed_at', F.timestamp_millis(F.col('source_changed_at'))
    )
    if cdc_df.filter(F.col('source_changed_at').isNull()).limit(1).count() > 0:
        raise ValueError('Data contains null changed at')
    return cdc_df