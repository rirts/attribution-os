{{ config(enabled=false) }}

select * from read_parquet('s3://dp-gold/web2_attribution/date=*/*.parquet')
