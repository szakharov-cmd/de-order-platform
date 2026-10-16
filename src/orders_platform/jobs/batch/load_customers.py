from pyspark.sql import SparkSession
import clickhouse_connect
import os

from orders_platform.transformations.customers import build_customers_state
from orders_platform.sinks.clickhouse import insert_customers_state

spark = (
    SparkSession.builder
    .appName('load-customers')
    .config('spark.sql.session.timeZone', 'UTC')
    .getOrCreate()
)

try:
    spark.sparkContext.setLogLevel('WARN')
    customers_df = spark.read.parquet('s3a://bronze/cdc/all-tables-v1/')
    customers_df = build_customers_state(cdc_df=customers_df)

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
        insert_customers_state(client=ch_client, state_df=customers_df)
    finally:
        ch_client.close()

finally:
    spark.stop()

