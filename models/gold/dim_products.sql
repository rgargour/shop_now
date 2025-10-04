{{ config(
      materialized='table',
      tags = ['gold']
) }}

select
  p.product_id,
  p.product_name,
  p.category_id,
  p.brand,
  p.supplier_id,
  p.list_price,
  p.cost_price,
  p.vat_rate,
  (p.list_price * (1 + coalesce(p.vat_rate,0))) as list_price_ttc,
  p.is_active,
  p.created_at,
  p.updated_at
from {{ ref('stg_products') }} p
