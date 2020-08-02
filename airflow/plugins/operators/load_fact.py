from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,

                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Clear table before use')
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info('Insert songplays from staging tables')
        table_name_string = '{}_table_insert'.format(self.table)
        redshift.run(getattr(SqlQueries, table_name_string).format(self.table))