select
    order_id,
    uniqExact(order_item_id) as line_count,
    sum(quantity) as item_quantity,
    sum(quantity * unit_price) as items_gross_amount,
    sum(discount_amount) as discount_amount,
    items_gross_amount - discount_amount as items_net_amount
from {{ ref('order_items') }}
group by order_id