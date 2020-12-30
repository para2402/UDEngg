import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import LoadDimensionOperator, DataQualityOperator

import sql


# Returns a DAG which loads data into that table from S3. When the load is 
# complete, a data quality  check is performed to assert that at least
# one row of data is present.
def get_load_verify_dimension_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        table,
        sql_query,
        truncate_before_load,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    
    copy_task = LoadDimensionOperator(
        task_id=f"load_{table}_from_staging",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table,
        sql_query=sql_query,
        truncate_before_load=truncate_before_load,
    )
    
    check_task = DataQualityOperator(
        task_id=f"check_{table}_dim_data",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table
    )
    
    copy_task >> check_task

    return dag