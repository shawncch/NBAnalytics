{{
  config(
    materialized = 'incremental',
    partitioned_by = ['season_id', 'season_type'],
    incremental_strategy = 'append'
  )
}}

{% if is_incremental() %}
  {% set max_date = get_max('game_date', 'nbanalytics_silver', 'stg_plays') %}
{% endif %}


with raw_plays as (
    select
        gameid as game_id
        , partition_2 as game_date
        , actionnumber as action_number
        , cast(substr(clock, 3, 2) as int) as minutes_left
        , cast(substr(clock, 6, 2) as int) as seconds_left
        , cast(substr(clock, 9, 2) as int) as miliseconds_left
        , period as quarter
        , teamid as team_id
        , teamtricode as team_tricode
        , personid as player_id
        , playernamei as player_name_shortened
        , xlegacy
        , ylegacy
        , shotdistance as shot_distance
        , shotresult as shot_result
        , isfieldgoal as is_fg
        , scorehome as score_home
        , scoreaway as score_away
        , location
        , description
        , case 
            when actiontype = '' and (description like '%BLOCK%' or description like '%Block%' or description like '%BLK%') then 'Block'  
            when actiontype = '' and (description like '%STEAL%' or description like '%Steal%' or description like '%STL%') then 'Steal'
            else actiontype  
          end as action_type
        , subtype
        , shotvalue as shot_value
        , actionid as action_id
        , partition_0 as season_id
        , partition_1 as season_type
        
    from {{ source('nba_raw', 'plays') }} 

    {% if is_incremental() %}
    where partition_2 >= coalesce('{{ max_date }}', '1900-01-01')
    {% endif %}
)

select 
    game_id
    ,game_date
    ,action_number
    ,minutes_left
    ,seconds_left
    ,miliseconds_left
    ,quarter
    ,team_id
    ,team_tricode
    ,player_id
    ,player_name_shortened
    ,xlegacy
    ,ylegacy
    ,shot_distance
    ,shot_result
    ,is_fg
    ,score_home
    ,score_away
    ,location
    ,description
    ,action_type
    ,subtype
    ,shot_value
    ,action_id
    ,season_id
    ,season_type
from raw_plays





