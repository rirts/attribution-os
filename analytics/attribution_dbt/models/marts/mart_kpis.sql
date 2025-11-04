with base as (
  select * from {{ ref('stg_orders') }}
)
select
  now()                                                          as as_of,
  count(*)                                                       as orders_count,
  sum(amount)                                                    as revenue_total,
  case when count(*) > 0 then sum(amount)::numeric / count(*) else 0 end as aov,
  count(distinct channel)                                        as channels_count
from base
