{% macro normalize_email(col) -%}
    lower(replace(regexp_replace({{ col }}, '\.\.', '.'), ' @', '@'))
{%- endmacro %}