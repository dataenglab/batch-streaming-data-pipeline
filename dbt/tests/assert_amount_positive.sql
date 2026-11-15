-- Singular test: сумма заказа после очистки должна быть положительной.
select order_id, amount
from {{ ref('stg_orders') }}
where amount <= 0
