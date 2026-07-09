{{
  config(
    materialized = 'incremental',
    unique_key = '',
    )
}}

with plays as (
    select
        *
    from {{ ref('stg_plays') }}
)

, home_score as (
    select distinct
        game_id
        , first_value(team_id) over (partition by game_id order by score_home) as home_team_id
    from plays
    where score_home != '0'
)



select
    plays.game_id
    , case when plays.team_id = home_score.home_team_id then 1 else 0 end as is_home_team
    , case when plays.quarter = 4 and minutes_left < 5 then 1 else 0 end as is_clutch
    , case 
        when action_type in ('Made Shot', 'Missed Shot') then 'fga'
        when action_type = 'Turnover' then 'tov'
        when action_type = 'Free Throw' then 'fta'
        when action_type = 'Rebound' and 
    {# , case when  #}
from plays
join home_score on plays.team_id = home_score.home_team_id

select distinct action_type from {{ ref('stg_plays') }}


{# special treatment for heave, for 25/26 season onwards heave fg attempts counts if made but does not count if made #}
select * from {{ ref('stg_plays') }} where game_id = '0042500205' order by action_number