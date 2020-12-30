from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """ COPY {table}
                   FROM '{s3_path}'
                   iam_role '{iam_role_arn}'
                   FORMAT AS json '{json_options}'
               """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 redshift_s3_iam_role_arn="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 log_json_file=None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.redshift_s3_iam_role_arn = redshift_s3_iam_role_arn
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data in {self.table} Redshift table")
        redshift_hook.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            iam_role_arn=self.redshift_s3_iam_role_arn,
            json_options="s3://{}/{}".format(self.s3_bucket, self.log_json_file) if self.log_json_file else 'auto'
        )
        
        redshift_hook.run(formatted_sql)
        self.log.info(f"Successfully staged '{self.table}' table !!")
        