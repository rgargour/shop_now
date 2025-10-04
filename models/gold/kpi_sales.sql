{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',   
    unique_key='order_month',         
    on_schema_change='sync_all_columns',
    tags = ['gold']
) }}

with monthly as (
  select
    order_month,
    count(*) as orders_count,
    sum(coalesce(amount, 0)) as revenue
  from {{ ref('fct_orders') }}
  where status_description = 'shipped'
  group by order_month
),

with_growth as (
  select
    order_month,
    orders_count,
    revenue,
    lag(revenue) over (order by order_month) as revenue_prev,
    case
      when lag(revenue) over (order by order_month) is null then null
      when lag(revenue) over (order by order_month) = 0 then null
      else (revenue - lag(revenue) over (order by order_month))
           / nullif(lag(revenue) over (order by order_month), 0)
    end as revenue_growth_rate
  from monthly
)

select * from with_growth
