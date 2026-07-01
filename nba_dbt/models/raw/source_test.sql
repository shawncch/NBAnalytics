{# {{
  config(
    materialized = '',
    )
}}

{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    )
}} #}
select partition_0 from {{ source('nba_raw', 'gamelogs') }}