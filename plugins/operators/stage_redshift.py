from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id = '',
                 aws_credentials_id ='',
                 s3_bucket='',
                 s3_key='',
                 json_path = '',
                 region='',
                 table = '',

                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id= aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region
        self.table = table

    def execute(self, context):
            self.log.info("Connecting to Redshift")
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
        
           
            redshift.run("DELETE FROM {}".format(self.table))

            rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            self.log.info("Built ther full S3 path")

            if self.file_format.upper() == "JSON":
                copy_sql = f"""
                    COPY {self.table}
                    FROM '{s3_path}'
                    ACCESS_KEY_ID '{credentials.access_key}'
                    SECRET_ACCESS_KEY '{credentials.secret_key}'
                    JSON '{self.json_path}'
                    REGION '{self.region}';
                """

            elif self.file_format.upper() == "CSV":
                copy_sql = f"""
                    COPY {self.table}
                    FROM '{s3_path}'
                    ACCESS_KEY_ID '{credentials.access_key}'
                    SECRET_ACCESS_KEY '{credentials.secret_key}'
                    CSV
                    IGNOREHEADER 1
                    REGION '{self.region}';
                """

            else:
                raise ValueError(f"Unsupported file format: {self.file_format}")

        
            self.log.info("Running the copy command")
            redshift.run(copy_sql)
            self.log.info(f"Staging complete for table {self.table}")