create table if not exists staging.refunds_cdc (
    refund_id Int64,
    payment_id Nullable(Int64),
    amount Nullable(Decimal(18,2)),
    status LowCardinality(Nullable(String)),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    succeeded_at Nullable(DateTime64(6, 'UTC')),
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