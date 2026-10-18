select *
from {{ ref('products') }}
where current_price < 0