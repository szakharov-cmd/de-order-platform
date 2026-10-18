select
    p.order_id as order_id,
    uniqExact(r.refund_id) as refund_attempt_count,
    uniqExactIf(r.refund_id, r.status = 'succeeded') as successful_refund_count,
    uniqExactIf(r.refund_id, r.status = 'failed') as failed_refund_count,
    sumIf(r.amount, r.status = 'succeeded') as refunded_amount
from {{ ref('refunds') }} r
inner join (
    select distinct
        payment_id, order_id
    from {{ ref('payments') }}
) p
    using (payment_id)
group by p.order_id