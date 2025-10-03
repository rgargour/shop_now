{{ config(materialized='view') }}
select 
 product_id,
	lower(product_name),
	category_id,
	lower(brand),
	supplier_id,
	list_price,
	cost_price,
	vat_rate,
	is_active,
	created_at,
	updated_at
from {{ source('ecommerce','products_raw') }}
where product_id is not null