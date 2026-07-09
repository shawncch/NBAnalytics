{{
  config(
    materialized = 'table',
    )
}}

with gamelogs as (
    select
        season_id,
        season_type,
        team_name,
        team_abbreviation,
        sum(case when wl = 'W' then 1 else 0 end) as no_wins,
        sum(case when wl = 'L' then 1 else 0 end) as no_losses

    from {{ ref('stg_gamelogs') }}
    group by 1,2,3,4
)

, ranked as (
    select
        *
        , dense_rank() over (partition by season_id, season_type order by no_wins desc) as rnk
    from gamelogs
)

select
    rnk,
    team_name,
    team_abbreviation,
    season_id,
    season_type,
    concat(cast(no_wins as varchar), '-', cast(no_losses as varchar)) as wl_record
from ranked
order by 4,1,5