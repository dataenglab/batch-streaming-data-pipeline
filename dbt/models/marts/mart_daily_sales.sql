select
    order_date,
    category,
    count(*) as orders_count,
    sum(amount) as total_amount,
    avg(amount) as avg_check
from {{ ref('stg_orders') }}
group by order_date, category
order by order_date