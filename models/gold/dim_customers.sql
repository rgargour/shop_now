{{ config(materialized='table') }}


SELECT 
*,
CONCAT(first_name, ' ', last_name) AS full_name

FROM {{ref('stg_customers')}}