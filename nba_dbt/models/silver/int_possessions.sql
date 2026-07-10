{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append'
    partitioned_by = 
    )
}}

{% if is_incremental() %}
  {% set max_date = get_max('game_date', 'nbanalytics_silver', 'int_possessions') %}
{% endif %}

with home_teams as (
    select distinct
        game_id as game_id
        , first_value(team_id) over (partition by game_id order by score_home) as team_id
    from {{ ref('stg_plays') }}
    where score_home != '0' and score_home != ''
)

, away_teams as (
    select distinct
        game_id as game_id
        , first_value(team_id) over (partition by game_id order by score_away) as team_id
    from {{ ref('stg_plays') }}
    where score_away != '0' and score_away != ''
)
, plays as (
    select
        stg_plays.game_id
        ,game_date
        ,action_number
        ,minutes_left
        ,seconds_left
        ,miliseconds_left
        ,home_teams.team_id as home_team_id
        ,away_teams.team_id as away_team_id
        {# , stg_plays.team_id as orig_team_id #}
        ,quarter
        ,case
            when action_type = 'Free Throw' and description like '%MISS%' then 0
            else 1
          end as is_ft_made
        , case
            when stg_plays.team_id = 0 and (location != '' or location is not null)
                then case 
                    when location = 'h' then home_teams.team_id
                    when location = 'v' then away_teams.team_id
                    else null
                end
            else stg_plays.team_id
          end as team_id
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
        ,lag(location) over (partition by stg_plays.game_id order by action_number) as prev_location
        ,lag(action_type) over (partition by stg_plays.game_id order by action_number) as prev_action_type
        {# after the first ft is missed, a 'normal' rebound event which has no team_tricode is recorded, this should not be considered a rebound #}
        ,lag(team_tricode) over (partition by stg_plays.game_id order by action_number) as prev_team_tricode 
        ,season_id
        ,season_type
    from {{ ref('stg_plays') }} stg_plays 
    left join home_teams on stg_plays.game_id = home_teams.game_id 
    left join away_teams on stg_plays.game_id = away_teams.game_id
)

, oreb as (
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
    from plays
)

, lead_actions as (
    select
        *
        ,lead(is_oreb) over (partition by game_id order by action_number) as is_next_action_oreb
        ,lead(action_type) over (partition by game_id order by action_number) as next_action_type
        ,lead(location) over (partition by game_id order by action_number) as next_location
        ,lead(action_type, 2) over (partition by game_id order by action_number) as next_next_action_type
        ,lead(location, 2) over (partition by game_id order by action_number) as next_next_location
    from oreb
)

select
    game_id
    , action_number
    , home_team_id
    , away_team_id
    , team_id
    , team_tricode
    , location
    , action_type
    , description
    , case when team_id = home_team_id then 1 else 0 end as is_home_team
    , case when quarter = 4 and minutes_left < 5 then 1 else 0 end as is_clutch
    , case
        when action_type = 'Made Shot' and next_action_type = 'Foul' and next_location != location and next_next_action_type = 'Turnover' and next_next_location != location then 1 -- turnover by other team via offensive foul after ft taken
        when action_type = 'Made Shot' and next_action_type = 'Foul' and next_location != location then 0 -- if shot is an and-1, possession will end after ft is taken
        when action_type = 'Made Shot' then 1 -- normal shot make leads to end of possession
        when action_type = 'Turnover' then 1 -- normal turnover leads to end of possession
        when action_type = 'Missed Shot' and is_next_action_oreb = 0 then 1 -- if shot is missed, and next event is a defensive rebound by the other team, end possession
        when action_type = 'Free Throw'
            then case
                when next_action_type in ('Turnover', 'Made Shot', 'Missed Shot') and next_location = location then 0 -- turnover from lane violations or made shot (after technical ft), possession counted in earlier cases
                when is_next_action_oreb = 0 and next_action_type not in  ('Free Throw', 'Substitution') then 1 -- no offensive rebound in next event, last ft is either made or missed, end possession
                else 0
            end
        else 0
      end as is_possession_end
    
from lead_actions

{# 
select distinct action_type from {{ ref('stg_plays') }}


{# special treatment for heave, for 25/26 season onwards heave fg attempts counts if made but does not count if made #}
select * from {{ ref('stg_plays') }} where game_id = '0042500205' order by action_number #}