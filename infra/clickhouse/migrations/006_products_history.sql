create table if not exists staging.products_cdc (
    product_id Int64,
    name Nullable(String),
    category Nullable(String),
    brand Nullable(String),
    current_price Nullable(Decimal(18,2)),
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