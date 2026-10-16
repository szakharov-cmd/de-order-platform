from pyspark.sql import (
    functions as F,
    DataFrame,
    Window
)
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType, 
    StringType
)

def build_customers_state(cdc_df: DataFrame) -> DataFrame:
    cdc_df = cdc_df.filter(F.col('topic') == 'shop.public.customers')

    key_schema = StructType([
        StructField("customer_id", LongType(), True),
    ])
    cdc_df = cdc_df.withColumn('parsed_key', F.from_json(F.col('key'), key_schema))

    customer_schema = StructType([
        StructField("customer_id", LongType(), True),
        StructField("email", StringType(), True),
        StructField("region", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])

    value_schema = StructType([
        StructField("op", StringType(), True),
        StructField("after", customer_schema, True),
    ])

    cdc_df = cdc_df.filter(F.col('value').isNotNull())
    cdc_df = cdc_df.withColumn('parsed_value', F.from_json(F.col('value'), value_schema))

    invalid_partitions = (
        cdc_df
        .groupBy('parsed_key.customer_id')
        .agg(
            F.countDistinct('partition').alias('partition_cnt')
        )
        .filter(F.col('partition_cnt') > 1)
    )
    if invalid_partitions.limit(1).count() > 0:
        invalid_partitions.show(20, truncate=False)
        raise ValueError(
            'One customer ID events in different partitions'
        )

    if cdc_df.filter(F.col('parsed_key.customer_id').isNull()).limit(1).count() > 0:
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
            (F.col('parsed_value.after.customer_id').isNull()) | (
                    F.col('parsed_key.customer_id') != F.col('parsed_value.after.customer_id'))
    ).limit(1).count() > 0:
        raise ValueError(
            'Customer ID value in key does not match value'
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

    window = Window.partitionBy('parsed_key.customer_id').orderBy(F.col('offset').desc())
    cdc_df = cdc_df.withColumn(
        'rn',
        F.row_number().over(window)
    )
    cdc_df = cdc_df.filter(F.col('rn') == 1)

    cdc_df = cdc_df.select(
        F.col('parsed_key.customer_id').alias('customer_id'),
        F.col('parsed_value.after.email').alias('email'),
        F.col('parsed_value.after.region').alias('region'),
        F.col('parsed_value.after.segment').alias('segment'),
        F.col('parsed_value.after.created_at').alias('created_at'),
        F.col('parsed_value.after.updated_at').alias('updated_at'),
        F.col('partition').alias('source_partition'),
        F.col('offset').alias('source_offset'),
        F.when(
            F.col('parsed_value.op') == 'd', F.lit(1))
        .otherwise(F.lit(0)
                   ).alias('is_deleted')
    )

    cdc_df = cdc_df.withColumns({
        c: F.to_timestamp(c) for c in ['created_at', 'updated_at']
    })
    return cdc_df