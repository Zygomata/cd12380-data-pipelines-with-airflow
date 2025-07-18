from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Custom operator to load data into a Redshift dimension table.
    Can optionally truncate the table before inserting.
    """

    ui_color = '#80BD9E'
    template_fields = ("sql",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f"Truncating dimension table {self.table} before loading new data")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql}
        """

        self.log.info(f"Running SQL for dimension table load:\n{insert_sql}")
        redshift.run(insert_sql)
        self.log.info(f"Finished loading data into dimension table: {self.table}")
