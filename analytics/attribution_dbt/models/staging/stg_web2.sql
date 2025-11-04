{{ config(enabled=false) }}

select * from read_parquet('s3://dp-silver/web2/date=*/*.parquet')
