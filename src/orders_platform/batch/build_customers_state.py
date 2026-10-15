from pyspark.sql import (
    SparkSession,
    functions as F,
    Window
)
from pyspark.sql.types import StructType, StructField, LongType, StringType


spark = (
    SparkSession.builder
    .appName('build-customer-state')
    .config('spark.sql.session.timeZone', 'UTC')
    .getOrCreate()
)

try:
    spark.sparkContext.setLogLevel('WARN')

    customer_df = spark.read.parquet('s3a://bronze/cdc/all-tables-v1/')
    customer_df = customer_df.filter(F.col('topic')=='shop.public.customers')

    key_schema = StructType([
        StructField("customer_id", LongType(), True),
    ])
    customer_df = customer_df.withColumn('parsed_key', F.from_json(F.col('key'), key_schema))

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

    customer_df = customer_df.filter(F.col('value').isNotNull())
    customer_df = customer_df.withColumn('parsed_value', F.from_json(F.col('value'), value_schema))

    invalid_partitions = (
        customer_df
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

    if customer_df.filter(F.col('parsed_key.customer_id').isNull()).limit(1).count() > 0:
        raise ValueError(
            'Data contains null keys'
        )

    if customer_df.filter(
            (F.col('parsed_value.op').isNull()) | (~F.col('parsed_value.op').isin(['c', 'r', 'u', 'd']))
    ).limit(1).count() > 0:
        raise ValueError(
            'Invalid table operation in data'
        )

    cru_operations = customer_df.filter(F.col('parsed_value.op').isin(['c', 'r', 'u']))
    if cru_operations.filter(F.col('parsed_value.after').isNull()).limit(1).count() > 0:
        raise ValueError(
            'Not after values for CRU operations'
        )
    if cru_operations.filter(
            (F.col('parsed_value.after.customer_id').isNull()) | (F.col('parsed_key.customer_id') != F.col('parsed_value.after.customer_id'))
    ).limit(1).count() > 0:
        raise ValueError(
            'Customer ID value in key does not match value'
        )

    if customer_df.filter(
           F.col('parsed_value.op').isin(['c','r','u']) & F.to_timestamp('parsed_value.after.created_at').isNull()
    ).limit(1).count() > 0:
        raise ValueError(
            'Created at value in null'
        )
    if customer_df.filter(
            F.col('parsed_value.op').isin(['c','r','u']) & F.to_timestamp('parsed_value.after.updated_at').isNull()
    ).limit(1).count() > 0:
        raise ValueError(
            'Updated at value in null'
        )

    window = Window.partitionBy('parsed_key.customer_id').orderBy(F.col('offset').desc())
    customer_df = customer_df.withColumn(
        'rn',
        F.row_number().over(window)
    )

    customer_df = customer_df.filter(F.col('rn')==1)
    customer_df = customer_df.filter(F.col('parsed_value.op') != 'd')

    customer_df = customer_df.select(
        F.col('parsed_key.customer_id').alias('customer_id'),
        F.col('parsed_value.after.email').alias('email'),
        F.col('parsed_value.after.region').alias('region'),
        F.col('parsed_value.after.segment').alias('segment'),
        F.col('parsed_value.after.created_at').alias('created_at'),
        F.col('parsed_value.after.updated_at').alias('updated_at')
    )

    customer_df = customer_df.withColumns({
        c: F.to_timestamp(c) for c in ['created_at', 'updated_at']
    })
    customer_df.show(truncate=False)

finally:
    spark.stop()
