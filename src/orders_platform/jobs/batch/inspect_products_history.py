from pyspark.sql import SparkSession
import clickhouse_connect
import os

from orders_platform.transformations.products import parse_products_cdc

spark = (
    SparkSession.builder
    .appName('load-customers')
    .config('spark.sql.session.timeZone', 'UTC')
    .getOrCreate()
)

try:
    spark.sparkContext.setLogLevel('WARN')
    product_df = spark.read.parquet('s3a://bronze/cdc/all-tables-v1/')
    product_df = parse_products_cdc(product_df)

    # Схема: имена колонок и типы
    product_df.printSchema()

    # История в порядке Kafka offsets
    product_df.orderBy("partition", "offset").show(
        100,
        truncate=False,
    )

    # Количество событий каждого типа
    product_df.groupBy("operation").count().orderBy("operation").show()

finally:
    spark.stop()