from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query = "",
                 truncate_before_load=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_before_load = truncate_before_load

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_before_load:
            self.log.info(f"Clearing data in {self.table} Redshift table")
            redshift_hook.run("DELETE FROM {}".format(self.table))
        
        redshift_hook.run(self.sql_query)
        self.log.info(f"Dimension table '{self.table}' loaded successfully !!")
