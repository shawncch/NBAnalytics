from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from nba_api.stats.endpoints import LeagueGameLog, LeagueGameFinder, PlayByPlayV3
import pandas as pd
from datetime import date, timedelta, datetime
from pendulum import duration

def get_latest_game():
    pass

def retrieve_games_for_day(game_date) -> list[pd.DataFrame]:
    games_for_day_df = LeagueGameFinder(
        league_id_nullable='00',
        date_from_nullable=game_date,
        date_to_nullable=game_date,
        ).get_data_frames()[0][['SEASON_ID', 'GAME_ID', 'GAME_DATE']].sort_values(by='GAME_ID').drop_duplicates()
    
    return games_for_day_df

def get_nba_season(game_date: str):
    game_date_converted = datetime.strptime(game_date, '%Y-%m-%d').date()
    if game_date_converted.month > 9:
        return f'{game_date_converted.year}-{str(game_date_converted.year + 1)[-2:]}'
    else:
        return f'{game_date_converted.year - 1}-{str(game_date_converted.year)[-2:]}'


def retrieve_plays_for_day(ti):
    game_list = ti.xcom_pull(task_ids='get_week_games')

    for index, row in game_list.iterrows():
        game_id = row['GAME_ID']
        game_date = row['GAME_DATE']
        season_id = row['SEASON_ID']
        nba_season = get_nba_season(game_date)
        if season_id[0] not in ['2', '4']:
            continue
        df = PlayByPlayV3(game_id=game_id).get_data_frames()[0]
        if season_id[0] == '2':
            df.to_parquet(f's3://nba-de/plays/{nba_season}/regular_season/{game_date}/{game_id}.parquet', index=False)
            print(f'game {game_id}\'s playbyplay data written successfully')
        else:
            df.to_parquet(f's3://nba-de/plays/{nba_season}/post_season/{game_date}/{game_id}.parquet', index=False)
            print(f'game {game_id}\'s playbyplay data written successfully')
            

default_args={
    'retries': 3,
    'retry_delay': duration(minutes=2),
    'retry_exponential_backoff': True
}

with DAG(
    dag_id='playbyplay_historical_load_dag',
    start_date=datetime(2004, 10, 1),
    end_date=datetime.now(),
    schedule= '0 0 * * 1', # every monday
    catchup=True,
    default_args=default_args,
    max_active_runs=5
):
    get_week_games = PythonOperator(
        task_id = 'get_week_games',
        python_callable=retrieve_games_for_day,
        op_kwargs={'game_date':'{{logical_date.strftime(\'%Y-%m-%d\')}}'}
    )

    get_plays = PythonOperator(
        task_id = 'get_gameplays',
        python_callable= retrieve_plays_for_day
    )
    

    get_week_games >> get_plays