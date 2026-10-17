with
table_data as (
    select *
    from {{ source('staging', 'payments_cdc') }} final
),
last_update as (
    select
        *,
        row_number() over(
            partition by payment_id
            order by offset desc
        ) as rn
    from table_data
)
select
    payment_id,
    order_id,
    amount,
    currency,
    status,
    payment_method,
    created_at,
    updated_at,
    succedeed_at,
    source_changed_at,
    ingested_at,
    partition as source_partition,
    offset as source_offset
from last_update
where rn = 1 and operation!='d'