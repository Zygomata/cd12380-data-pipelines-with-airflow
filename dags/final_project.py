from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)
from helpers import SqlQueries

# Default parameters for DAG behavior
default_args = {
    'owner': 'airflow_user',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Instantiate the DAG
@dag(
    default_args=default_args,
    description='ETL pipeline for Redshift using staged data from S3',
    schedule_interval='@hourly',
    catchup=False,
    tags=['final', 'redshift', 's3']
)
def redshift_etl_pipeline():

    # Begin DAG execution
    init_pipeline = DummyOperator(task_id='start_pipeline')

    # Stage log data
    load_event_staging = StageToRedshiftOperator(
        task_id='stage_event_data',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='wgu-d608-udacity-bucket',
        s3_key='log-data',
        json_path='auto',
        region='us-east-1',
        table='staging_events'
    )

    # Stage song data
    load_song_staging = StageToRedshiftOperator(
        task_id='stage_song_data',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='wgu-d608-udacity-bucket',
        s3_key='song-data',
        json_path='auto',
        region='us-east-1',
        table='staging_songs'
    )

    # Load fact table
    populate_songplays = LoadFactOperator(
        task_id='load_songplays_fact',
        redshift_conn_id='redshift',
        table='songplays',
        sql=SqlQueries.songplay_table_insert
    )

    # Load dimension tables
    populate_users = LoadDimensionOperator(
        task_id='load_users_dim',
        redshift_conn_id='redshift',
        table='users',
        sql_query=SqlQueries.user_table_insert,
        truncate_insert=True
    )

    populate_songs = LoadDimensionOperator(
        task_id='load_songs_dim',
        redshift_conn_id='redshift',
        table='songs',
        sql_query=SqlQueries.song_table_insert,
        truncate_insert=True
    )

    populate_artists = LoadDimensionOperator(
        task_id='load_artists_dim',
        redshift_conn_id='redshift',
        table='artists',
        sql_query=SqlQueries.artist_table_insert,
        truncate_insert=True
    )

    populate_time = LoadDimensionOperator(
        task_id='load_time_dim',
        redshift_conn_id='redshift',
        table='time',
        sql_query=SqlQueries.time_table_insert,
        truncate_insert=True
    )

    run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tests=[
        ("SELECT COUNT(*) FROM users WHERE userid IS NULL;", 0),
        ("SELECT COUNT(*) FROM songs;", 100),
        ("SELECT COUNT(*) FROM songplays;", 1),
    ]
)

    # Define pipeline structure
    init_pipeline >> [load_event_staging, load_song_staging] >> populate_songplays
    populate_songplays >> [populate_users, populate_songs, populate_artists, populate_time] >> run_quality_checks

# Register DAG
etl_dag = redshift_etl_pipeline()
