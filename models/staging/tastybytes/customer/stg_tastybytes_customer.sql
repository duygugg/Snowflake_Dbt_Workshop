with source as (
    select * from {{source('tastybytes','customer_loyalty')}}
)

select
    *
from source