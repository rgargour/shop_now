{{ config(
    materialized='incremental', 
    incremental_strategy='append',
    tags = ['gold']
) }}

with base as (
  select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status_description,
    o.amount,
    o.order_month,
    created_at
  from {{ ref('stg_orders') }} o
),

joined as (
  select
    b.order_id,
    b.order_date,
    b.order_month,
    b.status_description,
    b.amount,
    c.customer_id,
    c.full_name as customer_name,
    b.created_at
  from base b
  left join {{ ref('dim_customers') }} c on b.customer_id = c.customer_id
)

select * 
from joined
{% if is_incremental() %}
where created_at > (select max(created_at) from {{ this }})
{% endif %}
