from pyspark.sql import SparkSession
import clickhouse_connect
import os

from orders_platform.transformations.customers import parse_customers_cdc

spark = (
    SparkSession.builder
    .appName('load-customers')
    .config('spark.sql.session.timeZone', 'UTC')
    .getOrCreate()
)

try:
    spark.sparkContext.setLogLevel('WARN')
    customers_df = spark.read.parquet('s3a://bronze/cdc/all-tables-v1/')
    customers_df = parse_customers_cdc(customers_df)

    # Схема: имена колонок и типы
    customers_df.printSchema()

    # История в порядке Kafka offsets
    customers_df.orderBy("partition", "offset").show(
        100,
        truncate=False,
    )

    # Количество событий каждого типа
    customers_df.groupBy("operation").count().orderBy("operation").show()

finally:
    spark.stop()