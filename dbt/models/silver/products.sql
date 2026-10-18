with
table_data as (
    select *
    from {{ source('staging', 'products_cdc') }} final
),
last_update as (
    select
        *,
        row_number() over(
            partition by product_id
            order by offset desc
        ) as rn
    from table_data
)
select
    product_id,
    name,
    category,
    brand,
    current_price,
    created_at,
    updated_at,
    source_changed_at,
    ingested_at,
    partition as source_partition,
    offset as source_offset
from last_update
where rn = 1 and operation!='d'