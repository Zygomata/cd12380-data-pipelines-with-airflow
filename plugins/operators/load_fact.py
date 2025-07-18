from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    ui_color = '#F98866'
    template_fields = ("sql_query",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 target_table='',
                 sql_query='',
                 truncate_table=True,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql_query = sql_query
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info(f"Connecting to Redshift using connection ID: {self.redshift_conn_id}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info(f"Truncating target fact table: {self.target_table}")
            redshift.run(f"TRUNCATE TABLE {self.target_table}")

        full_insert_sql = f"""
            INSERT INTO {self.target_table}
            {self.sql_query}
        """

        self.log.info(f"Executing insert statement for fact table: {self.target_table}")
        redshift.run(full_insert_sql)
        self.log.info(f"Data load complete for fact table: {self.target_table}")
