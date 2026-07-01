{% macro get_max(col, schema, table_name) %}

    {% set query %}
        select max({{ col }}) from {{ schema }}.{{ table_name }}
    {% endset %}

    {% set result = dbt_utils.get_single_value(query) %}
    
    {{ log("THE MAX VALUE IS: " ~ result, info=True) }}
    
    {{ return(result) }}
{% endmacro %}