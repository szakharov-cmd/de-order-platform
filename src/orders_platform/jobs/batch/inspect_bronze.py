from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName('cdc-inspect-bronze')
    .config('spark.sql.session.timeZone', 'UTC')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

cdc_df = spark.read.parquet('s3a://bronze/cdc/all-tables-v1/')
cdc_df.cache()
cdc_df.printSchema()

cdc_df.select(
    'topic',
    'partition',
    'offset',
    'key',
    'value',
    'ingested_at',
    'ingest_date'
).orderBy('topic', 'partition', 'offset').show(100, truncate=False)

rows_num = cdc_df.groupBy('topic').count()
uniq_rows_num = (
    cdc_df.select('topic', 'partition', 'offset')
    .distinct()
    .groupBy('topic')
    .count()
)
null_val_num = cdc_df.filter(F.col('value').isNull()).groupBy('topic').count()

print(f"Total rows count: {rows_num.orderBy('topic').show()}")
print(f"Uniq rows count: {uniq_rows_num.orderBy('topic').show()}")
print(f"Null rows count: {null_val_num.orderBy('topic').show()}")

cdc_df.unpersist()
spark.stop()