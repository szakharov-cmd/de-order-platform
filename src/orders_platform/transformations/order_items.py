from pyspark.sql import (
    functions as F
)
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    IntegerType,
    DecimalType
)


def parse_order_items_cdc(cdc_df):
    cdc_df = cdc_df.filter(F.col('topic') == 'shop.public.order_items')
    if cdc_df.filter(
            F.col('partition').isNull() | (F.col('partition') != 0)
    ).limit(1).count() > 0:
        raise ValueError('Partition invalid')

    if cdc_df.filter(
            (F.col('offset') < 0) | F.col('offset').isNull()
    ).limit(1).count()>0:
        raise ValueError('Offset invalid')

    key_schema = StructType([
        StructField("order_item_id", LongType(), True),
    ])
    cdc_df = cdc_df.withColumn('parsed_key', F.from_json(F.col('key'), key_schema))

    order_items_schema = StructType([
        StructField("order_item_id", LongType(), True),
        StructField("order_id", LongType(), True),
        StructField("product_id", LongType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", StringType(), True),
        StructField("discount_amount", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])

    source_schema = StructType([
        StructField('ts_ms', LongType(), True)
    ])

    value_schema = StructType([
        StructField("source", source_schema, True),
        StructField("op", StringType(), True),
        StructField("after", order_items_schema, True),
    ])

    cdc_df = cdc_df.filter(F.col('value').isNotNull())
    cdc_df = cdc_df.withColumn('parsed_value', F.from_json(F.col('value'), value_schema))

    invalid_partitions = (
        cdc_df
        .groupBy('parsed_key.order_item_id')
        .agg(
            F.countDistinct('partition').alias('partition_cnt')
        )
        .filter(F.col('partition_cnt') > 1)
    )
    if invalid_partitions.limit(1).count() > 0:
        invalid_partitions.show(20, truncate=False)
        raise ValueError(
            'One order item ID events in different partitions'
        )

    if cdc_df.filter(F.col('parsed_key.order_item_id').isNull()).limit(1).count() > 0:
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
            (F.col('parsed_value.after.order_item_id').isNull()) | (
                    F.col('parsed_key.order_item_id') != F.col('parsed_value.after.order_item_id'))
    ).limit(1).count() > 0:
        raise ValueError(
            'Order item ID value in key does not match value'
        )
    if cdc_df.filter(
            F.col('parsed_value.op').isin(['c', 'r', 'u']) & F.col('parsed_value.after.order_id').isNull()
    ).limit(1).count() > 0:
        raise ValueError(
            'Order ID value in null'
        )
    if cdc_df.filter(
            F.col('parsed_value.op').isin(['c', 'r', 'u']) & F.col('parsed_value.after.product_id').isNull()
    ).limit(1).count() > 0:
        raise ValueError(
            'Product ID value in null'
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
        "quantity",
        F.col("parsed_value.after.quantity").cast(IntegerType())
    )
    if cdc_df.filter(
        F.col('parsed_value.op').isin(['c', 'r', 'u'])
        & (
            F.col('quantity').isNull() | (F.col('quantity') <= 0)
        )
    ).limit(1).count() > 0:
        raise ValueError('Quantity value in null')

    cdc_df = cdc_df.withColumn(
        "unit_price",
        F.col("parsed_value.after.unit_price").cast(DecimalType(18,2))
    )
    if cdc_df.filter(
        F.col('parsed_value.op').isin(['c', 'r', 'u'])
        & (
            F.col('unit_price').isNull() | (F.col('unit_price') < 0)
        )
    ).limit(1).count() > 0:
        raise ValueError('Unit price value in null')

    cdc_df = cdc_df.withColumn(
        "discount_amount",
        F.col("parsed_value.after.discount_amount").cast(DecimalType(18, 2))
    )
    if cdc_df.filter(
        F.col('parsed_value.op').isin(['c', 'r', 'u'])
        & (
            F.col('discount_amount').isNull() | (F.col('discount_amount') < 0)
        )
    ).limit(1).count() > 0:
        raise ValueError('Discount amount value in null')

    if cdc_df.filter(
        F.col('parsed_value.op').isin(['c', 'r', 'u'])
        & (
            F.col('quantity') * F.col('unit_price') < F.col('discount_amount')
        )
    ).limit(1).count() > 0:
        raise ValueError('Discount amount more than price')

    cdc_df = cdc_df.select(
        F.col('parsed_key.order_item_id').alias('order_item_id'),
        F.col('parsed_value.after.order_id').alias('order_id'),
        F.col('parsed_value.after.product_id').alias('product_id'),
        F.col('quantity').alias('quantity'),
        F.col('unit_price').alias('unit_price'),
        F.col('discount_amount').alias('discount_amount'),
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