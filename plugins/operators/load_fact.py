from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    truncate_sql = """
    TRUNCATE TABLE {};
    """
    insert_sql = """
    INSERT INTO {} {}
    WHERE songplay_id IS NOT NULL;
    """
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
        # Operators params (with defaults)
        redshift_conn_id = "redshift",
        table = "",
        truncate_data = False,
        sql_query = "",
        *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate_data = truncate_data
        self.sql_query = sql_query
        
    def execute(self):
        self.log.info(f"Started execution of LoadFactOperator on {self.table}. \
                      Delete existing data: {self.truncate_data}.")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
     
        if self.truncate_data:
            redshift_hook.run(LoadFactOperator.truncate_sql.format(self.table))

        redshift_hook.run(LoadFactOperator.insert_sql.format(self.table, self.sql_query))
        
        self.log.info(f"Completed implemenation of LoadFactOperator on {self.table}")
        