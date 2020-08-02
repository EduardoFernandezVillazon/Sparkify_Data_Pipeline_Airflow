from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date' : datetime(2018, 11, 30),
    #'start_date': datetime(2019, 4, 25),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5)
}

def create_tables(redshift_conn_id="redshift"):
    redshift = PostgresHook(postgres_conn_id=redshift_conn_id)
    redshift.run(SqlQueries.create_tables)    

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = PythonOperator(
    task_id='Begin_execution',
    python_callable=create_tables,
    dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    json_path="JSON 's3://udacity-dend/log_json_path.json'",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json", #/
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    json_path="""JSON 'auto' truncatecolumns""",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A/", # */*/*/*.json
    dag=dag
)

load_songplays_table = LoadDimensionOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="songplays",
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table="users",
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table="songs",
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table="artists",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table="time",
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    dq_checks=[
        {'check_sql_query':"SELECT COUNT(*) FROM users WHERE userid is null",'expected_result':0},
        {'check_sql_query':"SELECT COUNT(*) FROM songs WHERE songid is null",'expected_result':0},
        {'check_sql_query':"SELECT COUNT(*) FROM artists WHERE artistid is      null",'expected_result':0},
        {'check_sql_query':"SELECT COUNT(*) FROM songplays WHERE playid is null",'expected_result':0},
#        {'check_sql_query':"SELECT COUNT(*) FROM users",'expected_result': not 0},
#        {'check_sql_query':"SELECT COUNT(*) FROM songs",'expected_result': not 0},
#        {'check_sql_query':"SELECT COUNT(*) FROM artists",'expected_result': not 0},
#        {'check_sql_query':"SELECT COUNT(*) FROM songplays",'expected_result': not 0}
    ],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
load_songplays_table << [stage_events_to_redshift, stage_songs_to_redshift]
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
run_quality_checks << [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table ]
run_quality_checks >> end_operator