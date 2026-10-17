create table if not exists staging.orders_cdc (
    order_id Int64,
    customer_id Nullable(Int64),
    status LowCardinality(Nullable(String)),
    currency LowCardinality (Nullable(String)),
    shipping_amount Nullable(Decimal(18,2)),
    delivery_region Nullable(String),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    operation LowCardinality(String),
    constraint valid_operation
        check operation in ('r', 'c', 'u', 'd'),
    source_changed_at DateTime64(3, 'UTC'),
    topic LowCardinality(String),
    partition UInt32,
    offset UInt64,
    ingested_at DateTime64(6, 'UTC')
)
engine = ReplacingMergeTree()
ORDER BY (topic, partition, offset);