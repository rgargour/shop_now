{{ config(
      materialized='table',
      tags = ['gold']
) }}

select
  c.customer_id,
  c.first_name,
  c.last_name,
  concat(c.first_name, ' ', c.last_name) as full_name,
  c.email,
  c.loyalty_tier,
  c.customer_status,
  c.marketing_opt_in,
  c.address_line1,
  c.address_line2,
  c.city,
  c.postal_code,
  c.country,
  c.created_at,
  c.updated_at
from {{ ref('stg_customers') }} c
