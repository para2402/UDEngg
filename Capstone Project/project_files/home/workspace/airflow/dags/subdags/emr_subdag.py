from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

from helpers.emr.config_vars import SPARK_STEPS


def get_emr_subdag(parent_dag_name, child_dag_name, table,
                   script_name, task_type, job_flow_id,
                   aws_conn_id, **kwargs):
    """ 
        A factory function that returns a DAG with EMR steps and sensors
    """
    
    dag = DAG(f'{parent_dag_name}.{child_dag_name}', **kwargs)
    
    ROOT_BUCKET = Variable.get('ROOT_BUCKET')    
    RAW_DATA_KEY = Variable.get('RAW_DATA_KEY')
    STAGING_DATA_KEY = Variable.get('STAGING_DATA_KEY')
    SPARK_SCRIPTS_KEY = Variable.get('SPARK_SCRIPTS_KEY')
    
    RAW_DATA_PATH = f's3://{ROOT_BUCKET}/{RAW_DATA_KEY}'
    STAGING_DATA_PATH = f's3://{ROOT_BUCKET}/{STAGING_DATA_KEY}'
    SPARK_SCRIPTS_PATH = f's3://{ROOT_BUCKET}/{SPARK_SCRIPTS_KEY}'
    
    # Add steps to the EMR cluster
    table_steps_id = f'{task_type}_{table}_steps'
    table_steps = EmrAddStepsOperator(
        task_id=table_steps_id,
        job_flow_id=job_flow_id,
        aws_conn_id=aws_conn_id,
        steps=SPARK_STEPS,
        # `params` will be used by SPARK_STEPS
        params={
                'STEP_NAME': f'{task_type.title()} {table.title()} Table',
                'SPARK_SCRIPT': SPARK_SCRIPTS_PATH + f'/{script_name}.py',
                'RAW_DATA_PATH': RAW_DATA_PATH,
                'STAGING_DATA_PATH': STAGING_DATA_PATH,
                'TABLE': table
                },
        dag=dag,
    )

    # Probe EMR steps to check status
    # `final_step` value lets the sensor know the last step to watch
    final_step = len(SPARK_STEPS) - 1
    table_task_sensor = EmrStepSensor(
        task_id=f'{task_type}_{table}_sensor',
        job_flow_id=job_flow_id,
        step_id="{{ task_instance.xcom_pull(task_ids='" + table_steps_id + "', key='return_value')[" +  str(final_step) + "] }}",
        aws_conn_id=aws_conn_id,
        dag=dag,
    )
    
    table_steps >> table_task_sensor
    
    return dag