select
    o.order_id as order_id,
    o.customer_id as customer_id,
    o.status as order_status,
    o.currency as currency,
    o.delivery_region as delivery_region,
    o.created_at as created_at,
    o.updated_at as updated_at,
    c.email as customer_email,
    c.region as customer_region,
    c.segment as customer_segment,
    coalesce(i.line_count, 0) as line_count,
    coalesce(i.item_quantity, 0) as item_quantity,
    coalesce(i.items_gross_amount, 0) as items_gross_amount,
    coalesce(i.discount_amount, 0) as discount_amount,
    coalesce(i.items_net_amount, 0) as items_net_amount,
    coalesce(o.shipping_amount, 0) as shipping_amount,
    coalesce(i.items_net_amount, 0) + coalesce(o.shipping_amount, 0) as order_total_amount,
    coalesce(p.payment_attempt_count, 0) as payment_attempt_count,
    coalesce(p.successful_payment_count, 0) as successful_payment_count,
    coalesce(p.failed_payment_count, 0) as failed_payment_count,
    coalesce(p.paid_amount, 0) as paid_amount,
    coalesce(r.refund_attempt_count, 0) as refund_attempt_count,
    coalesce(r.successful_refund_count, 0) as successful_refund_count,
    coalesce(r.failed_refund_count, 0) as failed_refund_count,
    coalesce(r.refunded_amount, 0) as refunded_amount,
    paid_amount - refunded_amount as net_collected_amount,
    if(
        successful_payment_count > 0
        and paid_amount >= order_total_amount,
        1, 0
    ) as is_fully_paid,
    now('UTC') as built_at
from {{ ref('orders') }} o
left join {{ ref('customers') }} c
    on c.customer_id = o.customer_id
left join {{ ref('int_order_items_summary') }} i
    on i.order_id = o.order_id
left join {{ ref('int_order_payments_summary') }} p
    on p.order_id = o.order_id
left join {{ ref('int_order_refunds_summary') }} r
    on r.order_id = o.order_id
settings join_use_nulls = 1
