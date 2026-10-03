with source as (
    select * from raw_orders
),

cleaned as (
    select
        order_id,
        customer_name,
        product,
        category,
        cast(amount as float) as amount,
        case
            when order_date ~ '^\d{4}-\d{2}-\d{2}$' then order_date::date
            when order_date ~ '^\d{2}\.\d{2}\.\d{4}$' then to_date(order_date, 'DD.MM.YYYY')
            when order_date ~ '^\d{4}/\d{2}/\d{2}$' then to_date(order_date, 'YYYY/MM/DD')
            else null
        end as order_date
    from source
    where amount is not null
)

select distinct on (order_id) *
from cleaned
where order_date is not null
order by order_id, order_date desc