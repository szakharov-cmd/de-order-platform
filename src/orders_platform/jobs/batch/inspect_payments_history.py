from pyspark.sql import SparkSession
import clickhouse_connect
import os

from orders_platform.transformations.payments import parse_payments_cdc

spark = (
    SparkSession.builder
    .appName('load-payments')
    .config('spark.sql.session.timeZone', 'UTC')
    .getOrCreate()
)

try:
    spark.sparkContext.setLogLevel('WARN')
    payments_df = spark.read.parquet('s3a://bronze/cdc/all-tables-v1/')
    payments_df = parse_payments_cdc(payments_df)

    # Схема: имена колонок и типы
    payments_df.printSchema()

    # История в порядке Kafka offsets
    payments_df.orderBy("partition", "offset").show(
        100,
        truncate=False,
    )

    # Количество событий каждого типа
    payments_df.groupBy("operation").count().orderBy("operation").show()

finally:
    spark.stop()