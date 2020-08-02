from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) he
                 # Example:
                 redshift_conn_id = "",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks=dq_checks
        self.error_count=0
        self.failing_tests=[]

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info (self.error_count)
        for check in self.dq_checks:

            check_sql_query = check.get('check_sql_query')
            expected_result = check.get('expected_result')
            
            records = redshift.get_records(check_sql_query)[0]
            
            if expected_result != records[0]:
                self.error_count += 1
                self.log.info (self.error_count)
                self.failing_tests.append(check_sql_query)
                
        if self.error_count > 0:
            self.log.info ('Tests Failed:')
            self.log.info (self.failing_tests)
            raise ValueError ('Data quality check failed')
                            