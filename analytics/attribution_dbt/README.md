## Models
- stg_orders: typed/clean seed
- mart_revenue_daily: daily revenue by channel
- mart_revenue_monthly: monthly rollup
- mart_kpis: totals + growth deltas
## Run
$env:DBT_PROFILES_DIR="$(Get-Location)\profiles"
dbt seed --full-refresh && dbt build
