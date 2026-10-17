select *
from {{ ref('order_items') }}
where quantity <= 0
    or unit_price < 0
    or discount_amount < 0
    or discount_amount > quantity * unit_price