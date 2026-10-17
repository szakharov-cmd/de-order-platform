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


def parse_payments_cdc(cdc_df):
    cdc_df = cdc_df.filter(F.col('topic') == 'shop.public.payments')
    if cdc_df.filter(
            F.col('partition').isNull() | (F.col('partition') != 0)
    ).limit(1).count() > 0:
        raise ValueError('Partition invalid')

    if cdc_df.filter(
            (F.col('offset') < 0) | F.col('offset').isNull()
    ).limit(1).count()>0:
        raise ValueError('Offset invalid')

    key_schema = StructType([
        StructField("payment_id", LongType(), True),
    ])
    cdc_df = cdc_df.withColumn('parsed_key', F.from_json(F.col('key'), key_schema))

    payment_schema = StructType([
        StructField("payment_id", LongType(), True),
        StructField("order_id", LongType(), True),
        StructField("amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("succeeded_at", StringType(), True),
    ])

    source_schema = StructType([
        StructField('ts_ms', LongType(), True)
    ])

    value_schema = StructType([
        StructField("source", source_schema, True),
        StructField("op", StringType(), True),
        StructField("after", payment_schema, True),
    ])

    cdc_df = cdc_df.filter(F.col('value').isNotNull())
    cdc_df = cdc_df.withColumn('parsed_value', F.from_json(F.col('value'), value_schema))

    invalid_partitions = (
        cdc_df
        .groupBy('parsed_key.payment_id')
        .agg(
            F.countDistinct('partition').alias('partition_cnt')
        )
        .filter(F.col('partition_cnt') > 1)
    )
    if invalid_partitions.limit(1).count() > 0:
        invalid_partitions.show(20, truncate=False)
        raise ValueError(
            'One payment ID events in different partitions'
        )

    if cdc_df.filter(F.col('parsed_key.payment_id').isNull()).limit(1).count() > 0:
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
            (F.col('parsed_value.after.payment_id').isNull()) | (
                    F.col('parsed_key.payment_id') != F.col('parsed_value.after.payment_id'))
    ).limit(1).count() > 0:
        raise ValueError(
            'Payment ID value in key does not match value'
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

    if cdc_df.filter(
            F.col('parsed_value.op').isin(['c', 'r', 'u'])
            & F.col('parsed_value.after.status').isin(['pending', 'failed'])
            & F.col('parsed_value.after.succeeded_at').isNotNull()
    ).limit(1).count() > 0:
        raise ValueError(
            'Wrong succeded at'
        )

    if cdc_df.filter(
            F.col('parsed_value.op').isin(['c', 'r', 'u'])
            & F.col('parsed_value.after.status').isin(['succeeded'])
            & F.to_timestamp('parsed_value.after.succeeded_at').isNull()
    ).limit(1).count() > 0:
        raise ValueError(
            'Wrong succeded at'
        )

    if cdc_df.filter(
        F.col('parsed_value.op').isin(['c', 'r', 'u'])
        & (F.col('parsed_value.after.order_id').isNull())
    ).limit(1).count() > 0:
        raise ValueError('Order ID is null')

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
            F.col('parsed_value.after.status').isNull()
            | (~F.col('parsed_value.after.status').isin('pending', 'succeeded', 'failed'))
        )
    ).limit(1).count() > 0:
        raise ValueError('Status is null')

    if cdc_df.filter(
        F.col('parsed_value.op').isin(['c', 'r', 'u'])
        & (
            F.col('parsed_value.after.payment_method').isNull()
            | (~F.col('parsed_value.after.payment_method').isin('card', 'bank_transfer'))
        )
    ).limit(1).count() > 0:
        raise ValueError('Payment method is null')

    cdc_df = cdc_df.withColumn(
        "amount",
        F.col("parsed_value.after.amount").cast(DecimalType(18, 2))
    )
    if cdc_df.filter(
            F.col('parsed_value.op').isin(['c', 'r', 'u'])
            & (
                    F.col('amount').isNull() | (F.col('amount') <= 0)
            )
    ).limit(1).count() > 0:
        raise ValueError('Amount value in null')

    cdc_df = cdc_df.select(
        F.col('parsed_key.payment_id').alias('payment_id'),
        F.col('parsed_value.after.order_id').alias('order_id'),
        F.col('amount').alias('amount'),
        F.col('parsed_value.after.currency').alias('currency'),
        F.col('parsed_value.after.status').alias('status'),
        F.col('parsed_value.after.payment_method').alias('payment_method'),
        F.col('parsed_value.after.created_at').alias('created_at'),
        F.col('parsed_value.after.updated_at').alias('updated_at'),
        F.col('parsed_value.after.succeeded_at').alias('succeeded_at'),
        F.col('parsed_value.op').alias('operation'),
        F.col('parsed_value.source.ts_ms').alias('source_changed_at'),
        F.col('topic').alias('topic'),
        F.col('partition').alias('partition'),
        F.col('offset').alias('offset'),
        F.col('ingested_at').alias('ingested_at')
    )

    cdc_df = cdc_df.withColumns({
        c: F.to_timestamp(c) for c in ['created_at', 'updated_at', 'succeeded_at']
    })
    cdc_df = cdc_df.withColumn(
        'source_changed_at', F.timestamp_millis(F.col('source_changed_at'))
    )
    if cdc_df.filter(F.col('source_changed_at').isNull()).limit(1).count() > 0:
        raise ValueError('Data contains null changed at')
    return cdc_df