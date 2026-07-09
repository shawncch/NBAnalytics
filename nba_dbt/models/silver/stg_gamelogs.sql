{{
  config(
    materialized = 'incremental',
    partitioned_by = ['season_id', 'season_type','game_date'],
    incremental_strategy='insert_overwrite'
  )
}}

{# 1: Preseason game
2: Regular season game
3: All-Star weekend game
4: Playoff game
5: Play-in game  #}

{% if is_incremental() %}
  {% set max_date = get_max('game_date', 'nbanalytics_silver', 'stg_gamelogs') %}
{% endif %}

with raw_gamelogs as (
    select
        team_id
        , season_id as orig_season_id
        , partition_0 as season_id
        , partition_1 as season_type
        , game_id
        , game_date
        , team_abbreviation
        , team_name
        , matchup
        , wl
        , min
        , pts
        , fgm
        , fga
        , fg_pct
        , fg3m
        , fg3a
        , ftm
        , fta
        , ft_pct
        , oreb
        , dreb
        , reb
        , ast
        , stl
        , blk
        , tov
        , pf
        , plus_minus
    from {{ source('nba_raw', 'gamelogs') }}
    where substr(cast(season_id as varchar), 1, 1) in ('2','4','5')
    {% if is_incremental() %}
      and game_date >= coalesce('{{ max_date }}', '1900-01-01')
    {% endif %}

{# 
    incremental strategy: insert_overwrite
    model checks for new games
    old games -> overwrite game_date partition
    new games -> insert into new partition

    alternative: merge
    check for matches in date partition -> update when matched or else insert

 #}

)

select
    team_id
    , orig_season_id
    , game_id
    , team_abbreviation
    , team_name
    , matchup
    , wl
    , min
    , pts
    , fgm
    , fga
    , fg_pct
    , fg3m
    , fg3a
    , ftm
    , fta
    , ft_pct
    , oreb
    , dreb
    , reb
    , ast
    , stl
    , blk
    , tov
    , pf
    , plus_minus
    , cast(season_id as varchar) as season_id
    , cast(season_type as varchar) as season_type
    , cast(game_date as date) as game_date
from raw_gamelogs