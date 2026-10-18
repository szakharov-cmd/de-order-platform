select
    order_id,
    uniqExact(payment_id) as payment_attempt_count,
    uniqExactIf(payment_id, status = 'succeeded') as successful_payment_count,
    uniqExactIf(payment_id, status = 'failed') as failed_payment_count,
    sumIf(amount, status = 'succeeded') as paid_amount
from {{ ref('payments') }}
group by order_id