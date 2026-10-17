from pyspark.sql import (
    SparkSession,
    functions as F
)
from pyspark.sql.types import (
    StringType,
    StructField,
    LongType,
    StructType,
    TimestampType,
    DateType,
    IntegerType
)
import os
import clickhouse_connect
from orders_platform.transformations.customers import parse_customers_cdc
from orders_platform.transformations.orders import parse_orders_cdc
from orders_platform.transformations.order_items import parse_order_items_cdc
from orders_platform.transformations.payments import parse_payments_cdc
from orders_platform.transformations.refunds import parse_refunds_cdc
from orders_platform.sinks.clickhouse import (
    insert_customers_history,
    insert_orders_history,
    insert_order_items_history,
    insert_payments_history,
    insert_refunds_history
)


spark = (
    SparkSession.builder
    .appName('load-staging-history-s3')
    .config('spark.sql.session.timeZone', 'UTC')
    .getOrCreate()
)

try:
    spark.sparkContext.setLogLevel('WARN')
    stream_schema = StructType([
        StructField('key', StringType(), True),
        StructField('value', StringType(), True),
        StructField('topic', StringType(), True),
        StructField('partition', IntegerType(), True),
        StructField('offset', LongType(), True),
        StructField('kafka_timestamp', TimestampType(), True),
        StructField('ingested_at', TimestampType(), True),
        StructField('ingest_date', DateType(), True),
    ])
    cdc_stream = (
        spark.readStream
        .schema(stream_schema)
        .option("maxFilesPerTrigger", 10)
        .parquet('s3a://bronze/cdc/all-tables-v1/')
    )

    def process_batch(batch, batch_id):

        ch_host = os.environ['CLICKHOUSE_HOST']
        ch_port = int(os.environ['CLICKHOUSE_PORT'])
        ch_user = os.environ['CLICKHOUSE_USER']
        ch_pass = os.environ['CLICKHOUSE_PASSWORD']
        ch_dbname = os.environ['CLICKHOUSE_DATABASE']

        ch_client = clickhouse_connect.get_client(
            host=ch_host,
            port=ch_port,
            user=ch_user,
            password=ch_pass,
            database=ch_dbname
        )

        try:
            customers_df = parse_customers_cdc(batch)
            orders_df = parse_orders_cdc(batch)
            order_items_df = parse_order_items_cdc(batch)
            payments_df = parse_payments_cdc(batch)
            refunds_df = parse_refunds_cdc(batch)

            inserted_rows = insert_customers_history(ch_client, customers_df)
            print(
                f"Customers microbatch {batch_id}: sent {inserted_rows} rows",
                flush=True,
            )
            inserted_rows = insert_orders_history(ch_client, orders_df)
            print(
                f"Orders microbatch {batch_id}: sent {inserted_rows} rows",
                flush=True,
            )
            inserted_rows = insert_order_items_history(ch_client, order_items_df)
            print(
                f"Order items microbatch {batch_id}: sent {inserted_rows} rows",
                flush=True,
            )
            inserted_rows = insert_payments_history(ch_client, payments_df)
            print(
                f"Payments microbatch {batch_id}: sent {inserted_rows} rows",
                flush=True,
            )
            inserted_rows = insert_refunds_history(ch_client, refunds_df)
            print(
                f"Refunds microbatch {batch_id}: sent {inserted_rows} rows",
                flush=True,
            )
        finally:
            ch_client.close()

    query = (
        cdc_stream
        .writeStream
        .foreachBatch(process_batch)
        .outputMode('append')
        .option(
            "checkpointLocation",
            "/checkpoints/staging-history-s3-v1",
        )
        .trigger(processingTime='10 seconds')
        .start()
    )

    try:
        query.awaitTermination()
    finally:
        query.stop()

finally:
    spark.stop()