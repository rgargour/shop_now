{{ config(materialized='table') }}


SELECT 
*,
cost_price * (1 + vat_rate) AS price_with_vat

FROM {{ref('stg_products')}}