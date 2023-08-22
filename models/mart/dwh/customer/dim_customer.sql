with customer as(
    select * from {{ref('stg_tastybytes_customer')}}
)

select 
    customer_key,
    first_name,
    {% if table_exists(this.database,this.schema,this.table) == 1 %}
        CASE 
            WHEN t2.customer_key is not null THEN cte.last_name
            ELSE t2.last_name
        end as last_name,
    {% else %}
        last_name,
  {% endif %}
    city,
    country,
    postal_code,
    preferred_language,
    gender,
    marital_status,
    children_count,
    register_date,
    birthday_date,
    email,
    phone_number

from customer
{% if table_exists(this.database,this.schema,this.table) == 1 %}
    LEFT JOIN {{this.database}}.{{this.schema}}.{{this.table}} as t2
    ON customer.customer_key = t2.customer_key
{% endif %}