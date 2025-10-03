{{ config(materialized='incremental',
    incremental_strategy='append', 
    on_schema_change='sync_all_columns'
    ) }}

select 
    order_id,
	customer_id,
	cast(order_date as date) as order_date,
    substr(cast(order_date as varchar), 6, 2) as order_month,
	orders.status,
    status_description,
	amount,
	created_at
 from {{ source('ecommerce','orders_raw') }} as orders join
 {{ ref('status_mapping') }} as status
 on orders.status=status.status
 where order_id is not null and customer_id is not null

 {% if is_incremental() %}
 and created_at > (select max(created_at) from {{this}})
 {% endif %}