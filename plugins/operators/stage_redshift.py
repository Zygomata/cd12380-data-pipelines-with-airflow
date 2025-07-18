from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 s3_bucket='',
                 s3_key='',
                 file_format='json',  
                 json_path='auto',
                 region='us-west-2',
                 destination_table='',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.json_path = json_path
        self.region = region
        self.destination_table = destination_table

    def execute(self, context):
        self.log.info(f"Starting staging process for {self.destination_table}")

        # Get Redshift and AWS credentials
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        creds = aws_hook.get_credentials()

        # Prepare dynamic S3 key
        resolved_key = self.s3_key.format(**context)
        s3_uri = f"s3://{self.s3_bucket}/{resolved_key}"

        self.log.info(f"Resolved S3 path: {s3_uri}")

        # Clear existing data
        self.log.info(f"Clearing data from Redshift table {self.destination_table}")
        redshift.run(f"DELETE FROM {self.destination_table}")

        # Construct COPY SQL based on format
        if self.file_format.lower() == 'json':
            copy_stmt = f"""
                COPY {self.destination_table}
                FROM '{s3_uri}'
                ACCESS_KEY_ID '{creds.access_key}'
                SECRET_ACCESS_KEY '{creds.secret_key}'
                JSON '{self.json_path}'
                REGION '{self.region}';
            """
        elif self.file_format.lower() == 'csv':
            copy_stmt = f"""
                COPY {self.destination_table}
                FROM '{s3_uri}'
                ACCESS_KEY_ID '{creds.access_key}'
                SECRET_ACCESS_KEY '{creds.secret_key}'
                CSV
                IGNOREHEADER 1
                REGION '{self.region}';
            """
        else:
            raise ValueError(f"Unsupported format '{self.file_format}'. Use 'json' or 'csv'.")

        # Execute COPY command
        self.log.info(f"Executing COPY command for {self.destination_table}")
        redshift.run(copy_stmt)

        self.log.info(f"Staging complete for table: {self.destination_table}")
