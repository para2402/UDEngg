from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from airflow.providers.postgres.operators.postgres import PostgresOperator

from operators.redshift_quality_check import RedshiftQualityCheckOperator

from helpers.emr.config_vars import JOB_FLOW_OVERRIDES
from subdags.emr_subdag import get_emr_subdag


default_args = {
    'owner': 'Olist',
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),    
#     'template_searchpath': ['templates/']
}

DAG_NAME = 'OLIST_ETL'
dag = DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval='@once',
    max_active_runs=1
)


START = DummyOperator(task_id="START", dag=dag)


######################### Spark Processing on EMR ##################################

# # Create an EMR cluster
# cluster_launch_task_id = 'launch_emr_cluster'
# launch_emr_cluster = EmrCreateJobFlowOperator(
#     task_id=cluster_launch_task_id,
#     job_flow_overrides=JOB_FLOW_OVERRIDES,
#     aws_conn_id="aws_default",
#     emr_conn_id="emr_default",
#     dag=dag,
# )


tables = ['customer', 'seller', 'product', 'calender', 'order_item']
# # tables = ['seller']
# generate_table_tasks = dict()
# staging_quality_checks = dict()
# for table in tables:
#     # Add EMR steps to process and generate dimension & fact tables
#     task_type = 'generate'
#     curr_task_name = f'{task_type}_{table}_table'
#     generate_table_tasks[table] = SubDagOperator(
#         task_id=curr_task_name,
#         subdag=get_emr_subdag(
#             parent_dag_name=DAG_NAME,
#             child_dag_name=curr_task_name,
#             table=table,
#             script_name=table,
#             task_type=task_type,
#             job_flow_id="{{ task_instance.xcom_pull(task_ids='" + cluster_launch_task_id + "', dag_id='" + DAG_NAME + "' , key='return_value') }}",
#             aws_conn_id='aws_default',
#             start_date=default_args['start_date']
#         ),
#         dag=dag,
#     )
    
#     # Data Quality Checks
#     task_type = 'check'
#     curr_task_name = f'{task_type}_{table}_table'
#     staging_quality_checks[table] = SubDagOperator(
#         task_id=curr_task_name,
#         subdag=get_emr_subdag(
#             parent_dag_name=DAG_NAME,
#             child_dag_name=curr_task_name,
#             table=table,
#             script_name='quality_checks',
#             task_type=task_type,
#             job_flow_id="{{ task_instance.xcom_pull(task_ids='" + cluster_launch_task_id + "', dag_id='" + DAG_NAME + "' , key='return_value') }}",
#             aws_conn_id='aws_default',
#             start_date=default_args['start_date']
#         ),
#         dag=dag,
#     )


# # Terminate the EMR cluster
# terminate_emr_cluster = EmrTerminateJobFlowOperator(
#     task_id="terminate_emr_cluster",
#     job_flow_id="{{ task_instance.xcom_pull(task_ids='" + cluster_launch_task_id + "', dag_id='" + DAG_NAME + "' , key='return_value') }}",
#     aws_conn_id="aws_default",
#     trigger_rule="all_done",
#     dag=dag,
# )




################# Loading Data Into REDSHIFT Tables For Analystics #################

# Create Redshift Warehouse Tables
create_tables = PostgresOperator(
    task_id='create_redshift_tables',
    default_args=default_args,
    sql='/templates/create_tables.sql',
    postgres_conn_id='redshift_connection',
    autocommit=True
)


ROOT_BUCKET = Variable.get('ROOT_BUCKET')
STAGING_DATA_KEY = Variable.get('STAGING_DATA_KEY')
REDSHIFT_S3_ROLE = Variable.get('REDSHIFT_S3_ROLE')

copy_to_redshift_tasks = dict()
redshift_quality_checks = dict()
for table in tables:
    # Move transformed data from S3 staging bucket to Redshift tables
    copy_to_redshift_tasks[table] = S3ToRedshiftOperator(
        task_id = f'copy_{table}_to_redshift',
        default_args=default_args,
        schema = 'PUBLIC',
        table = table,
        s3_bucket = ROOT_BUCKET,
        s3_key = f'{STAGING_DATA_KEY}/{table}',
        redshift_conn_id = 'redshift_connection',
        aws_conn_id = 'aws_default',
        copy_options = ['FORMAT AS', 'PARQUET'],
        truncate_table = True if table == 'order_item' else False
    )
    
    # Data Quality Checks
    redshift_quality_checks[table] = RedshiftQualityCheckOperator(
        task_id=f'check_{table}_redshift_table',
        default_args=default_args,
        table=table,
        postgres_conn_id='redshift_connection'
    )


END = DummyOperator(task_id="END", dag=dag)


################################ TASK DEPENDENCIES #################################

# START >> launch_emr_cluster

# for table in tables:
#     launch_emr_cluster >> generate_table_tasks[table]
#     generate_table_tasks[table] >> staging_quality_checks[table]
#     staging_quality_checks[table] >> terminate_emr_cluster
#     terminate_emr_cluster >> copy_to_redshift_tasks[table]
#     copy_to_redshift_tasks[table] >> redshift_quality_checks[table]
#     redshift_quality_checks[table]  >> END

START >> create_tables
for table in tables:
    create_tables >> copy_to_redshift_tasks[table] >> redshift_quality_checks[table] >> END



