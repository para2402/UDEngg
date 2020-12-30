from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (CreateTablesOperator, StageToRedshiftOperator,
                               LoadFactOperator, DataQualityOperator)

from load_verify_dims_subdag import get_load_verify_dimension_dag
from airflow.operators.subdag_operator import SubDagOperator

from helpers import SqlQueries

DAG_NAME = 'udac_example_dag'
S3_BUCKET = 'udacity-dend'

REDSHIFT_CONN_ID = 'redshift'
REDSHIFT_S3_IAM_ROLE = Variable.get('redshiftS3Role')


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
#     'start_date': datetime.utcnow()
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(DAG_NAME,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = CreateTablesOperator(
    task_id='Create_all_redshift_tables',
    create_sql_script="/home/workspace/airflow/create_tables.sql",
    redshift_conn_id = REDSHIFT_CONN_ID,
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    redshift_s3_iam_role_arn=REDSHIFT_S3_IAM_ROLE,
    table="staging_events",
    s3_bucket=S3_BUCKET,
    s3_key="log_data",
    log_json_file="log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    redshift_s3_iam_role_arn=REDSHIFT_S3_IAM_ROLE,
    table="staging_songs",
    s3_bucket=S3_BUCKET,
    s3_key="song_data",
    log_json_file=None
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table='songplays',
    sql_query=SqlQueries.songplay_table_insert,
)

check_songplays_table = DataQualityOperator(
    task_id="check_songplays_fact_data",
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table='songplays'
)

users_task_id = "load_users_dimension"
load_user_dimension_table = SubDagOperator(
    subdag=get_load_verify_dimension_dag(
        parent_dag_name=DAG_NAME,
        redshift_conn_id=REDSHIFT_CONN_ID,
        start_date=default_args['start_date'],
        task_id=users_task_id,
        table="users",
        sql_query=SqlQueries.user_table_insert,
        truncate_before_load=True
    ),
    task_id=users_task_id,
    dag=dag,
)

songs_task_id = "load_songs_dimension"
load_song_dimension_table = SubDagOperator(
    subdag=get_load_verify_dimension_dag(
        parent_dag_name=DAG_NAME,
        redshift_conn_id=REDSHIFT_CONN_ID,
        start_date=default_args['start_date'],
        task_id=songs_task_id,
        table="songs",
        sql_query=SqlQueries.song_table_insert,
        truncate_before_load=True
    ),
    task_id=songs_task_id,
    dag=dag,
)

artists_task_id = "load_artists_dimension"
load_artist_dimension_table = SubDagOperator(
    subdag=get_load_verify_dimension_dag(
        parent_dag_name=DAG_NAME,
        redshift_conn_id=REDSHIFT_CONN_ID,
        start_date=default_args['start_date'],
        task_id=artists_task_id,
        table="artists",
        sql_query=SqlQueries.artist_table_insert,
        truncate_before_load=True
    ),
    task_id=artists_task_id,
    dag=dag,
)

time_task_id = "load_time_dimension"
load_time_dimension_table = SubDagOperator(
    subdag=get_load_verify_dimension_dag(
        parent_dag_name=DAG_NAME,
        redshift_conn_id=REDSHIFT_CONN_ID,
        start_date=default_args['start_date'],
        task_id=time_task_id,
        table="time",
        sql_query=SqlQueries.time_table_insert,
        truncate_before_load=True
    ),
    task_id=time_task_id,
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


## TASK DEPENDENCY
start_operator >> create_tables

create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> check_songplays_table

[stage_events_to_redshift, stage_songs_to_redshift] >> load_user_dimension_table
[stage_events_to_redshift, stage_songs_to_redshift] >> load_song_dimension_table
[stage_events_to_redshift, stage_songs_to_redshift] >> load_artist_dimension_table

check_songplays_table >> load_time_dimension_table

[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> end_operator