{{ config(
    schema = 'stg',
    materialized = 'table',
    tags = ['staging']
) }}

with src as (
    select *
    from {{ source('ecommerce', 'products_raw') }}

), 
typed as (
    select
        cast(product_id as bigint)            as product_id,
        lower(trim(cast(product_name as text))) as product_name,
        cast(category_id as bigint)           as category_id,
        lower(trim(cast(brand as text)))      as brand,
        cast(supplier_id as bigint)           as supplier_id,
        cast(list_price as decimal(18,2))     as list_price,
        cast(cost_price as decimal(18,2))     as cost_price,
        cast(vat_rate   as decimal(9,4))      as vat_rate,
        cast(is_active  as boolean)           as is_active,
        cast(created_at as timestamp)         as created_at,
        cast(updated_at as timestamp)         as updated_at
    from src
), 
filtered as (
    select *
    from typed
    where product_id is not null
      and (list_price is null or list_price >= 0)
      and (cost_price is  null or cost_price  >= 0)
)

select *
from filtered
