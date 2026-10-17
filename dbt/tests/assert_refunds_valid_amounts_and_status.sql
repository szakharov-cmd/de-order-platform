select *
from {{ ref('refunds') }}
where amount <= 0
    or (status = 'succeeded' and succeeded_at is null)
    or (status != 'succeeded' and succeeded_at is not null)