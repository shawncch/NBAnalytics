from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from nba_api.stats.endpoints import LeagueGameLog, LeagueGameFinder, PlayByPlayV3
import pandas as pd
from datetime import date, timedelta, datetime
from pendulum import duration
import boto3
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator


def get_nba_season(game_date: str):
    game_date_converted = datetime.strptime(game_date, '%Y-%m-%d').date()
    if game_date_converted.month > 9:
        return f'{game_date_converted.year}-{str(game_date_converted.year + 1)[-2:]}'
    else:
        return f'{game_date_converted.year - 1}-{str(game_date_converted.year)[-2:]}'

def get_last_ingestion_date():
    curr_season = get_nba_season(datetime.now().date().strftime('%Y-%m-%d'))
    s3_client = boto3.client('s3')

    objects_reg = s3_client.list_objects_v2(Bucket='nba-de', Prefix=f'plays/{curr_season}/regular_season/', Delimiter='/')
    objects_post = s3_client.list_objects_v2(Bucket='nba-de', Prefix=f'plays/{curr_season}/post_season/', Delimiter='/')

    reg_dates = [p['Prefix'].split('/')[-2] for p in objects_reg.get("CommonPrefixes", [])]
    post_dates = [p['Prefix'].split('/')[-2] for p in objects_post.get("CommonPrefixes", [])]

    reg_dates.extend(post_dates)
    return reg_dates[-1]

def get_new_ingestion_date():
    last_ing_date = get_last_ingestion_date()
    new_ing_date = datetime.strptime(last_ing_date, '%Y-%m-%d') + timedelta(days=1)

    return new_ing_date


def retrieve_games_for_day(game_date) -> list[pd.DataFrame]:
    game_date = datetime.strptime(game_date, '%Y-%m-%d') - timedelta(days = 1)
    game_date = str(game_date.date())
    games_for_day_df = LeagueGameFinder(
        league_id_nullable='00',
        date_from_nullable=game_date,
        date_to_nullable=game_date,
        ).get_data_frames()[0][['SEASON_ID', 'GAME_ID', 'GAME_DATE']].sort_values(by='GAME_ID').drop_duplicates()
    
    print(games_for_day_df)
    
    return games_for_day_df

# 1: Preseason game
# 2/6: Regular season game
# 3: All-Star weekend game
# 4: Playoff game
# 5: Play-in game 


def retrieve_plays_for_day(ti):
    game_list = ti.xcom_pull(task_ids='get_day_games')
    for index, row in game_list.iterrows():
        game_id = row['GAME_ID']
        game_date = row['GAME_DATE']
        season_id = row['SEASON_ID']
        nba_season = get_nba_season(game_date)
        if season_id[0] not in ['2', '4', '5', '6']:
            continue
        df = PlayByPlayV3(game_id=game_id).get_data_frames()[0]
        if season_id[0] in ['2', '6']:
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

print('test', f'{type(get_new_ingestion_date())}')
# print(get_new_ingestion_date())

with DAG(
    dag_id='playbyplay_daily_load_dag',
    # start_date=get_new_ingestion_date(),
    start_date=datetime(2024, 10, 1),
    catchup=True,
    schedule='0 7 * * *', # 4pm daily
    default_args=default_args,
    max_active_tasks=5
):
    get_day_games = PythonOperator(
        task_id = 'get_day_games',
        python_callable=retrieve_games_for_day,
        op_kwargs={'game_date':'{{logical_date.strftime(\'%Y-%m-%d\')}}'}

    )

    get_plays = PythonOperator(
        task_id = 'get_gameplays',
        python_callable= retrieve_plays_for_day
    )
    
    # trigger_glue_crawler = GlueCrawlerOperator(
    #     task_id = 'trigger_gamelogs_crawler',
    #     config={'Name':'nba_plays'}
    # )
    

    # get_day_games >> get_plays >> trigger_glue_crawler
    get_day_games >> get_plays
