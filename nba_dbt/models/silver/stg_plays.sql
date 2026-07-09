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





with home_teams as (
    select distinct
        gameid as game_id
        , first_value(teamid) over (partition by gameid order by scorehome) as team_id
    from {{ source('nba_raw', 'plays') }}
    where scorehome != '0' and scorehome != ''
)

, away_teams as (
    select distinct
        gameid as game_id
        , first_value(teamid) over (partition by gameid order by scoreaway) as team_id
    from {{ source('nba_raw', 'plays') }}
    where scoreaway != '0' and scoreaway != ''
)

, raw_plays as (

    select
        gameid as game_id
        , partition_2 as game_date
        , actionnumber as action_number
        , cast(substr(clock, 3, 2) as int) as minutes_left
        , cast(substr(clock, 6, 2) as int) as seconds_left
        , cast(substr(clock, 9, 2) as int) as miliseconds_left
        , home_teams.team_id as home_team_id
        , away_teams.team_id as away_team_id
        , source_plays.teamid as orig_team_id
        , case
            when actiontype = 'Free Throw' and description like '%MISS%' then 0
            else 1
          end as is_ft_made
        , period as quarter
        , case
            when source_plays.teamid = 0 and (location != '' or location is not null)
                then case 
                    when location = 'h' then home_teams.team_id
                    when location = 'v' then away_teams.team_id
                    else null
                end
            else source_plays.teamid
          end as team_id
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
        , lag(location) over (order by actionnumber) as prev_location
        , lag(actiontype) over (order by actionnumber) as prev_action_type
        {# after the first ft is missed, a 'normal' rebound event which has no team_tricode is recorded, this should not be considered a rebound #}
        , lag(teamtricode) over (order by actionnumber) as prev_team_tricode 
        
    from {{ source('nba_raw', 'plays') }} source_plays
    left join home_teams on source_plays.gameid = home_teams.game_id 
    left join away_teams on source_plays.gameid = away_teams.game_id


    {% if is_incremental() %}
    where partition_2 >= coalesce('{{ max_date }}', '1900-01-01')
    {% endif %}
)

select 
    *
    , case when action_type = 'Rebound' and location = prev_location
        then case
            when prev_action_type = 'Missed Shot' then 1
            when prev_action_type = 'Free Throw' and (prev_team_tricode != '' or prev_team_tricode is not null) then 1
            else 0
            end 
        else 0
      end as is_oreb
from raw_plays





