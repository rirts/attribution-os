{% snapshot orders_snapshot %}
{{
  config(
    target_schema='public_',
    unique_key='order_id',
    strategy='check',
    check_cols=['amount','channel'],
    invalidate_hard_deletes=True
  )
}}
select
  order_id,
  channel,
  amount,
  order_date,
  order_ts
from {{ source('attrib','orders_seed') }}
{% endsnapshot %}
