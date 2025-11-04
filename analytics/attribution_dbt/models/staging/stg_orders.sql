select
	order_id,
    cast(order_date as date) as order_date,
    lower(channel) as channel,
    cast(amount as numeric(12,2)) as amount
from {{ ref('orders_seed') }}