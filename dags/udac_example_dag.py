from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'max_active_runs': 1
}

redshift_conn_id="redshift"
aws_credentials_id="aws_credentials"
s3_bucket="udacity-dend"
s3_region="us-west-2"

dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='59 23 * * *'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    aws_credentials_id=aws_credentials_id,
    table="staging_events",
    s3_bucket=s3_bucket,
    s3_key="log_data/{{execution_date.strftime('%Y')}}/{{execution_date.strftime('%m')}}/{{execution_date.strftime('%Y-%m-%d')}}-events.json",
    s3_region="us-west-2",
    json_format="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    aws_credentials_id=aws_credentials_id,
    table="staging_songs",
    s3_bucket=s3_bucket,
    s3_key="song_data",
    s3_region="us-west-2",
    json_format="auto"    
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table="songplays",
    select_sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table="users",
    primary_key="userid",
    select_sql=SqlQueries.user_table_insert,
    upsert=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table="songs",
    primary_key="songid",
    select_sql=SqlQueries.song_table_insert,    
    upsert=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table="artists",
    primary_key="artistid",
    select_sql=SqlQueries.artist_table_insert, 
    upsert=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    table="time",
    primary_key="start_time",
    select_sql=SqlQueries.time_table_insert, 
    upsert=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id=redshift_conn_id,
    table="songplays", 
    test_sql="SELECT COUNT(*) FROM songplays WHERE start_time BETWEEN '{{ ds }}' AND '{{ macros.ds_add(ds, 1) }}'",
    result_checker=(lambda result: (result > 0)),
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
run_quality_checks >> end_operator