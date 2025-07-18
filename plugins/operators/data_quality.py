from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = '',
                 tests= None,
                 expected= None,

                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []
        self.expected = expected or []

        if len(self.test_queries) != len(self.expected_results):
            raise ValueError("The number of test queries and expected results must be equal")



    def execute(self, context):
        self.log.info("Connecting to Redshift")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        errors = []
        
        for i, (query, expected_result) in enumerate(zip(self.test_queries, self.expected_results)):
            self.log.info(f"Running test {i+1}: {query}")
            records = redshift_hook.get_records(query)

            if not records or not records[0]:
               error_message = f"Data quality check fails for query: {query} - No records found."
               self.log.error(error_message)
               raise ValueError(error_message)

            
            result = records [0][0]
            if result != expected_result:
                error_message = f"Data quality check failed for query: {query}. Expected: {expected_result}, received: {result}"
                self.log.error(error_message)
                raise ValueError(error_message)
        
       
        self.log.info('Data Quality Checks Have Passed')