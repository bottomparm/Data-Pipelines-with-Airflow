from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    data_quality_checks = [
        {'check_sql' : "SELECT COUNT(*) FROM users WHERE userid IS NULL", 'expected_result' : 0},
        {'check_sql' : "SELECT COUNT(*) FROM songs WHERE songid IS NULL", 'expected_result' : 0},
        {'check_sql' : "SELECT COUNT(*) FROM artists WHERE artistid IS NULL", 'expected_result' : 0},
        {'check_sql' : "SELECT COUNT(*) FROM time WHERE start_time IS NULL", 'expected_result' : 0},
        {'check_sql' : "SELECT COUNT(*) FROM users WHERE userid IS NULL", 'expected_result' : 0}]

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
        # Define your operators params (with defaults)
        redshift_conn_id="redshift",
        tables=[],
        *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Begin data quality check...')
        
        err_count = 0
        failed_tests = []
        for check in DataQualityOperator.data_quality_checks:
            sql_query = check.get('check_sql')
            exp_result = check.get('expected_result')
            records = redshift_hook.get_records(sql_query)[0]
            
            if exp_result != records[0]:
                err_count += 1
                failed_tests.append(sql_query)
            
            if err_count > 0:
                self.log.info("Data quality check failed.")
                self.log.info("No. of failed tests: {}".format(err_count))
                self.log.info(failed_tests)
                raise ValueError("Data quality check failed.")
                
            if err_count == 0:
                self.log.info("Data quality check passed!")
