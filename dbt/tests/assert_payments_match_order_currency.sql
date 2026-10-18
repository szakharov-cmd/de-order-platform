select
    p.payment_id,
    p.order_id,
    p.currency as payment_currency,
    o.currency as order_currency
from {{ ref('payments') }} p
inner join {{ ref('orders') }} o
    on p.order_id = o.order_id
where p.currency != o.currency