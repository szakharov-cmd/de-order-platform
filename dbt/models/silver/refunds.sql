with
table_data as (
    select *
    from {{ source('staging', 'refunds_cdc') }} final
),
last_update as (
    select
        *,
        row_number() over(
            partition by refund_id
            order by offset desc
        ) as rn
    from table_data
)
select
    refund_id,
    payment_id,
    amount,
    status,
    created_at,
    updated_at,
    succeeded_at,
    source_changed_at,
    ingested_at,
    partition as source_partition,
    offset as source_offset
from last_update
where rn = 1 and operation!='d'