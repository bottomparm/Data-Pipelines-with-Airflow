from datetime import datetime, timedelta
import os
# import sys
# sys.path.append('..')

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
# from plugins.operators.stage_redshift import StageToRedshiftOperator
# from plugins.operators.load_fact import LoadFactOperator
# from plugins.operators.load_dimension import LoadDimensionOperator
# from plugins.operators.data_quality import DataQualityOperator
# from plugins.helpers.sql_queries import SqlQueries
from helpers import SqlQueries


AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'dhrebenach',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'email_on_retry' : False,
    'catchup' : False,
}

dag = DAG('sparkify_dag',
    default_args = default_args,
    description = 'Load and transform data in Redshift with Airflow',
    schedule_interval = '0 * * * *'
)

creates_tables_task = PostgresOperator(
    task_id = 'create_tables',
    dag = dag,
    sql = 'create_tables.sql',
    postgres_conn_id = 'redshift'
)

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_events',
    dag = dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = 'staging_events',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data/2018/11/'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_songs',
    dag = dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = 'staging_songs',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data/A/A/A/'
)

load_songplays_table = LoadFactOperator(
    task_id = 'load_songplays_fact_table',
    dag = dag,
    table = 'songplays',
    truncate_data = False,
    sql_query = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'load_user_dim_table',
    dag = dag,
    table = "users",
    truncate_data = True,
    sql_query = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'load_song_dim_table',
    dag = dag,
    table = "songs",
    truncate_data = True,
    sql_query = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'load_artist_dim_table',
    dag = dag,
    table = "artists",
    truncate_data = True,
    sql_query = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'load_time_dim_table',
    dag = dag,
    table = "time",
    truncate_data = True,
    sql_query = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id = 'run_data_quality_checks',
    dag = dag
)

end_operator = DummyOperator(task_id = 'stop_execution', dag = dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
