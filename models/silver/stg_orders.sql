{{ config(
    materialized='incremental', 
    incremental_strategy='append',
    tags = ['staging']
) }}

with src as (
    select 
      order_id,
      customer_id,
      order_date,
      status,
      amount,
      created_at
    from {{ source('ecommerce', 'orders_raw') }}
), 

typed as (
  select
    cast(order_id as bigint)        as order_id,
    try_cast(customer_id as bigint)     as customer_id,
    cast(order_date as date)        as order_date,
    date_trunc('month', order_date) as order_month,
    cast(status as varchar)   as status,
    cast(amount as float)   as amount,
    cast(created_at   as timestamp) as created_at,
  from src
  where order_id    is not null
    and customer_id is not null
),

final as (
  select
    c.order_id,
    c.customer_id,
    c.order_date,
    c.status, 
    coalesce(s.status_description, 'unknown') as status_description,                                    
    c.amount,
    c.order_month,
    c.created_at
  from typed c
  left join {{ref('status_mapping')}} s
  on s.status = c.status
                               
)
select *
from final

{% if is_incremental() %}
where created_at > (select max(created_at) from {{ this }})
{% endif %}

