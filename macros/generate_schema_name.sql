{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if not custom_schema_name -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name }}
  {%- endif -%}
{%- endmacro %}