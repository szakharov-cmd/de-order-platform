create table if not exists staging.order_items_cdc (
    order_item_id Int64,
    order_id Nullable(Int64),
    product_id Nullable(Int64),
    quantity Nullable(Int32),
    unit_price Nullable(Decimal(18,2)),
    discount_amount Nullable(Decimal(18,2)),
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