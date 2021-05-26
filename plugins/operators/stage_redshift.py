from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}';
    """
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                # Operators params (with defaults)
                redshift_conn_id="",
                table = "",
                aws_credentials_id="",
                s3_bucket="",
                s3_key = "",
                json_path="auto",
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Deleting data from {} table.".format(self.table))
        redshift_hook.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        staged_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )
        redshift_hook.run(staged_sql)
