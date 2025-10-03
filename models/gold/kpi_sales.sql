{{ config(
        materialized='incremental',
        incremental_strategy='delete+insert', 
        unique_key='order_month',
        on_schema_change='sync_all_columns'
    ) }}


SELECT 
    order_month,
    SUM(amount) AS total_sales,
    COUNT(*) AS total_orders,
    SUM(amount) - LAG(SUM(amount),1) OVER (ORDER BY order_month)/ NULLIF(SUM(amount), 0) AS sales_growth
FROM {{ref('fct_orders')}}
GROUP BY order_month
ORDER BY order_month