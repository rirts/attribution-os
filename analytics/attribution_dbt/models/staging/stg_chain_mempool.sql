{{ config(enabled=false) }}

select * from read_parquet('s3://dp-silver/chain_mempool/date=*/*.parquet')
