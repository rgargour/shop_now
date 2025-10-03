{{ config(materialized='table') }}


SELECT 
*
FROM {{ref('stg_orders')}} AS stg_orders
JOIN {{ref('dim_customers')}} AS dim_customers ON stg_orders.customer_id = dim_customers.customer_id