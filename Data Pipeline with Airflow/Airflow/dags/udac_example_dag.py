from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')


_s3_bucket = "udacity-dend"
_log_data_key  = "log_data"
_song_data_key = "song_data/A/A"
_redshift_conn_id = "redshift"
_aws_credentials_id = "aws_credentials"


default_args = {
    "owner": "DucAnhLai",
    "start_date": datetime(2021, 1, 12),
    #The DAG does not have dependencies on past runs
    "depends_on_past" : False,
    #On failure, the task are retried 3 times
    "retries": 3,
    #Do not email on retry
    "email_on_retry": False,
    #Retries happen every 5 minutes
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}


dag = DAG("dag",
          default_args=default_args,
          description="Load and transform data in Redshift with Airflow",
          schedule_interval ='@hourly'
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
    


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id= _redshift_conn_id,
    aws_credentials_id= _aws_credentials_id,
    s3_bucket=_s3_bucket,
    s3_key=_log_data_key,
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    table="staging_songs",
    redshift_conn_id= _redshift_conn_id,
    aws_credentials_id= _aws_credentials_id,
    s3_bucket=_s3_bucket,
    s3_key=_song_data_key,
    provide_context=True
)
    

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id= _redshift_conn_id,
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id= _redshift_conn_id,
    sql=SqlQueries.user_table_insert,
    mode=LoadDimensionOperator.append
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    redshift_conn_id= _redshift_conn_id,
    sql=SqlQueries.song_table_insert,
    mode=LoadDimensionOperator.append
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id= _redshift_conn_id,
    sql=SqlQueries.artist_table_insert,
    mode=LoadDimensionOperator.append
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    redshift_conn_id= _redshift_conn_id,
    sql=SqlQueries.time_table_insert,
    mode=LoadDimensionOperator.append
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id= _redshift_conn_id,
    tables=["songplays",
            "users",
            "songs",
            "artists",
            "time"]
)

end_operator = DummyOperator(task_id="Stop_execution",  dag=dag)
    
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