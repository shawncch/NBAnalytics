{% test check_season_ids(model, column_name) %}
    select
        {{ column_name }}
    from {{ model }}
    where substr(cast({{ column_name }} as varchar), 1, 1) not in ('1','2','3','4','5','6')

{% endtest %}