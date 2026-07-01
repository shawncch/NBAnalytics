from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from nba_api.stats.endpoints import LeagueGameLog, LeagueGameFinder, PlayByPlayV3, leaguegamefinder
import pandas as pd
from datetime import date, timedelta, datetime
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator


def retrieve_games_for_day(game_date) -> pd.DataFrame:
    game_date = datetime.strptime(game_date, '%Y-%m-%d') - timedelta(days = 1)
    game_date = str(game_date.date())
    return LeagueGameFinder(
        league_id_nullable = '00',
        date_from_nullable = game_date,
        date_to_nullable = game_date
    ).get_data_frames()[0]

def get_nba_season(game_date: str):
    game_date_converted = datetime.strptime(game_date, '%Y-%m-%d').date()
    if game_date_converted.month > 9:
        return f'{game_date_converted.year}-{str(game_date_converted.year + 1)[-2:]}'
    else:
        return f'{game_date_converted.year - 1}-{str(game_date_converted.year)[-2:]}'

def write_to_s3(ti):
    games = ti.xcom_pull('get_game_logs')


    if games.empty:
        return 'no gamelogs for today'


    game_date = games.loc[:,'GAME_DATE'][0]
    nba_season = get_nba_season(game_date)
    season_id = games.loc[:,'SEASON_ID'][0]


    print(games)

    if season_id[0] in ['2', '6']:
        games.to_csv(f's3://nba-de/gamelogs/{nba_season}/regular_season/{game_date}.csv', index=False)
        print(f'game logs data for {game_date} written successfully')
    
    elif season_id[0] in ['4', '5']:
        games.to_csv(f's3://nba-de/gamelogs/{nba_season}/post_season/{game_date}.csv', index=False)
        print(f'game logs data for {game_date} written successfully')
    
    else:
        print('there are games today but not regular/post season, skipping write to s3')
    


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True
}


with DAG(
    dag_id='daily_load_gamelogs_dag',
    default_args=default_args,
    start_date=datetime(2004, 10, 1),
    schedule = '0 7 * * *',
    catchup=True,
    max_active_tasks=4
):

    get_game_logs = PythonOperator(
        task_id = 'get_game_logs',
        python_callable=retrieve_games_for_day,
        op_kwargs={'game_date': '{{ logical_date.strftime(\'%Y-%m-%d\') }}'}
    )

    write_to_s3 = PythonOperator(
        task_id = 'print_game',
        python_callable = write_to_s3
    )

    # trigger_glue_crawler = GlueCrawlerOperator(
    #     task_id = 'trigger_gamelogs_crawler',
    #     config={'Name':'nba_gamelogs-copy'}
    # )
    
    # get_game_logs >> write_to_s3 >> trigger_glue_crawler
    get_game_logs >> write_to_s3 
