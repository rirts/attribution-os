# Attribution OS (seed MVP)

## Quickstart
1) Docker
   docker compose -f docker/compose.dev.yml up -d
2) Adminer: http://localhost:8081  |  Metabase: http://localhost:3000
3) dbt
   cd analytics/attribution_dbt
   \C:\Users\danie\attribution-os\analytics\attribution_dbt\profiles = "\C:\Users\danie\attribution-os\profiles"
   dbt seed --full-refresh
   dbt build
