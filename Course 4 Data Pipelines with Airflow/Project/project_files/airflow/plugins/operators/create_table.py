from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
    """ 
        Executes the CREATE table SQL statements in the file
        passed to through the `create_sql_script` argument
        while instantiating the operator.
    """
    
    @apply_defaults
    def __init__(self,
                 create_sql_script="",
                 redshift_conn_id = "",
                 *args, **kwargs):
        
        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_sql_script = create_sql_script
        
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        queries =  open(self.create_sql_script, 'r').read()
        redshift_hook.run(queries)
        
        self.log.info("Tables created successfully !! ")
