from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark = (
    SparkSession.builder 
    .appName('cdc-archive-debug') 
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

query = (
    streaming_df.writeStream
    .format('console')
    .outputMode('append')
    .option('truncate', 'false')
    .option('numRows', '100')
    .option('checkpointLocation', '/checkpoints/customers-console-v1')
    .trigger(processingTime='5 seconds')
    .start()
)
query.awaitTermination()