from pyspark.sql import SparkSession
import clickhouse_connect
import os

from orders_platform.transformations.orders import parse_orders_cdc

spark = (
    SparkSession.builder
    .appName('load-orders')
    .config('spark.sql.session.timeZone', 'UTC')
    .getOrCreate()
)

try:
    spark.sparkContext.setLogLevel('WARN')
    orders_df = spark.read.parquet('s3a://bronze/cdc/all-tables-v1/')
    orders_df = parse_orders_cdc(orders_df)

    # Схема: имена колонок и типы
    orders_df.printSchema()

    # История в порядке Kafka offsets
    orders_df.orderBy("partition", "offset").show(
        100,
        truncate=False,
    )

    # Количество событий каждого типа
    orders_df.groupBy("operation").count().orderBy("operation").show()

finally:
    spark.stop()