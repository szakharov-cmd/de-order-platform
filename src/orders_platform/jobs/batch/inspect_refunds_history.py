from pyspark.sql import SparkSession
import clickhouse_connect
import os

from orders_platform.transformations.refunds import parse_refunds_cdc

spark = (
    SparkSession.builder
    .appName('load-refunds')
    .config('spark.sql.session.timeZone', 'UTC')
    .getOrCreate()
)

try:
    spark.sparkContext.setLogLevel('WARN')
    refunds_df = spark.read.parquet('s3a://bronze/cdc/all-tables-v1/')
    refunds_df = parse_refunds_cdc(refunds_df)

    # Схема: имена колонок и типы
    refunds_df.printSchema()

    # История в порядке Kafka offsets
    refunds_df.orderBy("partition", "offset").show(
        100,
        truncate=False,
    )

    # Количество событий каждого типа
    refunds_df.groupBy("operation").count().orderBy("operation").show()

finally:
    spark.stop()