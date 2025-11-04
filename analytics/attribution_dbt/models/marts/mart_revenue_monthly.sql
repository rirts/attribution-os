with daily as (
  select order_date, channel, amount
  from {{ ref('stg_orders') }}
)
select
  date_trunc('month', order_date)::date                          as month,
  sum(amount)                                                    as revenue,
  sum(case when channel='ads'    then amount else 0 end)         as revenue_ads,
  sum(case when channel='email'  then amount else 0 end)         as revenue_email,
  sum(case when channel='direct' then amount else 0 end)         as revenue_direct
from daily
group by 1
order by 1
