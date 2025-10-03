{{ config(materialized='view') }}

select
distinct on (customer_id)
    customer_id,
	first_name,
	last_name,
	upper(trim(email)) as email,
    {{ normalize_email('email') }} as email_norm_maccro,
	address_line1,
	address_line2,
	city,
	postal_code,
	country,
	loyalty_tier,
	customer_status,
	marketing_opt_in,
	created_at,
	updated_at

from {{ source('ecommerce','customers_raw') }}
where customer_id is not null and email  is not null