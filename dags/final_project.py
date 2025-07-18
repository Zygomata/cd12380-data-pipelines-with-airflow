from datetime import datetime, timedelta
import  pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'wguuser',
    'depends_on_past': False,        
    'start_date': datetime(2024, 7, 1),
    'retries': 3,                     
    'retry_delay': timedelta(minutes=5),  
    'email_on_failure': False,        
    'email_on_retry': False          
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id = 'redshift',
        aws_credentials_id ='aws_credentials',
        s3_bucket='wgu-d608-udacity-bucket',
        s3_key='log-data',
        json_path = 'auto',
        region='us-east-1',
        table = 'staging_events',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id = 'redshift',
        aws_credentials_id ='aws_credentials',
        s3_bucket='wgu-d608-udacity-bucket',
        s3_key='log-data',
        json_path = 'auto',
        region='us-east-1',
        table = 'staging_events',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id = 'redshift',
        table='songplays',
        sql= SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table="users",
        sql_query=SqlQueries.user_table_insert,
        truncate_insert=True,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table="songs",
        sql_query=SqlQueries.song_table_insert,
        truncate_insert=True,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table="artists",
        sql_query=SqlQueries.artist_table_insert,
        truncate_insert=True,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table="time",
        sql_query=SqlQueries.time_table_insert,
        truncate_insert=True,
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        test_queries= [
            "SELECT COUNT(*) FROM songplays WHERE playid IS NULL",
            "SELECT COUNT(*) FROM users WHERE userid IS NULL"
        ],
        expected_results=[0, 0],
    )

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table,load_time_dimension_table , load_song_dimension_table, load_artist_dimension_table] >> run_quality_checks 



final_project_dag = final_project()

