{{config(
    materialized = "table"
)}}

with source as (
    select * from {{source('tastybytes_customer','customer_loyalty')}}
)

select
    customer_id as customer_key,
    first_name,
    last_name,
    city,
    country,
    postal_code,
    preferred_language,
    gender,
    case 
        when marital_status = 'Divorced/Seperated' then 'Divorced'
        else marital_status
    end as marital_status,
    children_count,
    sign_up_date as register_date,
    birthday_date,
    e_mail as email,
    phone_number
from source