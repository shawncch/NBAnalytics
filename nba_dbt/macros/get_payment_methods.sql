{% macro get_table_values() %}
  {% set payment_query %}
    select distinct
        payment_method
    from {{ source('nba_raw', 'sample_transactions') }}
  {% endset %}

  {{return(run_query(payment_query))}}
{% endmacro %}

{% macro get_payment_methods() %}
    {% if execute %}
        {{return(get_table_values().columns[0].values())}}

    {% else %}
        {{return([])}}
    
    {% endif %}
    
{% endmacro %}
