{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Use the custom schema name exactly as specified in dbt_project.yml,
        without prepending the default target schema. This gives us clean
        schema names like OURA_STAGING instead of OURA_MARTS_OURA_STAGING.
    #}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
