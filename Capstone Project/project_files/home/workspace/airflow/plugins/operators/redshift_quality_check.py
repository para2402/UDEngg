import logging
from typing import Iterable, Mapping, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

class RedshiftQualityCheckOperator(BaseOperator):
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        *,
        table: str,
        postgres_conn_id: str = 'postgres_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table = table
        self.postgres_conn_id = postgres_conn_id
        self.hook = None

    def execute(self, context):
        logging.info('Executing: %s', self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        count = self.hook.get_records(f"SELECT COUNT(*) FROM public.{self.table}")
        
        if not count or count < 0:
            err_msg = f"Data quality check FAILED. Table '{self.table}' returned zero or no results"
            logging.error(err_msg)
            raise ValueError(err_msg)
        
        logging.info(f"Data quality check PASSED on '{self.table}'.")
