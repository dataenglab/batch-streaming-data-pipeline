-- Singular test: в витрине не должно быть двух строк на одну пару (order_date, category).
-- Тест падает, если запрос вернул хотя бы одну строку.
select
    order_date,
    category,
    count(*) as rows_per_grain
from {{ ref('mart_daily_sales') }}
group by order_date, category
having count(*) > 1
