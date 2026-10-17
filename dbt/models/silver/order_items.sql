with
table_data as (
    select *
    from {{ source('staging', 'order_items_cdc') }} final
),
last_update as (
    select
        *,
        row_number() over(
            partition by order_item_id
            order by offset desc
        ) as rn
    from table_data
)
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    created_at,
    updated_at,
    source_changed_at,
    ingested_at,
    partition as source_partition,
    offset as source_offset
from last_update
where rn = 1 and operation!='d'