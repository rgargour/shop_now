{{ config(
    materialized = 'table',
    tags = ['staging']
) }}

with src as (
    select *
    from {{ source('ecommerce', 'customers_raw') }}
), 
normalized as (
    select
        customer_id,
        first_name,
        last_name,
        {{ normalize_email('email') }} as email,
        address_line1,
        address_line2,
        city,
        postal_code,
        country,
        loyalty_tier,
        customer_status,
        marketing_opt_in,
        cast(created_at as timestamp)  as created_at,
        cast(updated_at as timestamp)  as updated_at
    from src
), 
ranked as (
    select
        *,
        row_number() over (
          partition by customer_id
          order by created_at desc, updated_at desc
        ) as rn
    from normalized
    where email is not null and email <> ''
)

select
    customer_id,
    first_name,
    last_name,
    email,        
    address_line1,
    address_line2,
    city,
    postal_code,
    country,
    loyalty_tier,
    customer_status,
    marketing_opt_in,
    created_at,
    updated_at
from ranked
where rn = 1
