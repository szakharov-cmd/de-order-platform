with
success_refunds as (
    select
        payment_id,
        sum(amount) as refund_sum
    from {{ ref('refunds') }}
    where status = 'succeeded'
    group by payment_id
)
select payment_id
from success_refunds
where payment_id not in (select payment_id from {{ ref('payments') }})

union all

select payment_id
from success_refunds r
inner join {{ ref('payments') }} p
    using (payment_id)
where p.status != 'succeeded'
    or r.refund_sum > p.amount
