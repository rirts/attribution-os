select
  order_date,
  channel,
  sum(amount) as revenue
from {{ ref('stg_orders') }}
group by 1,2
