from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark = (
    SparkSession.builder 
    .appName('cdc-archive-bronze') 
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

streaming_df = (
    spark.readStream
    .format('kafka')
    .option('kafka.bootstrap.servers', 'kafka:9092')
    .option('subscribe', 'shop.public.customers')
    .option('startingOffsets', 'earliest')
    .load()
)

streaming_df = streaming_df.select('key', 'value', 'topic', 'partition', 'offset', 'timestamp')

streaming_df = streaming_df.withColumns({
    c: F.col(c).cast(StringType()) for c in ['key', 'value']
})
streaming_df = streaming_df.withColumnRenamed('timestamp', 'kafka_timestamp')
streaming_df = streaming_df.withColumn('ingested_at', F.current_timestamp())
streaming_df = streaming_df.withColumn('ingest_date', F.current_date())


query = (
    streaming_df.writeStream
    .format('parquet')
    .outputMode('append')
    .option('checkpointLocation', '/checkpoints/customers-parquet-v1')
    .option('path', 's3a://bronze/cdc/customers/')
    .partitionBy('ingest_date')
    .trigger(processingTime='30 seconds')
    .start()
)
query.awaitTermination()