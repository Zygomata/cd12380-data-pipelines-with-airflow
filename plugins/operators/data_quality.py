from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Executes one or more data quality checks on a Redshift table.
    Each test is a tuple of (query, expected_result).
    """

    ui_color = '#89DA59'
    template_fields = ('tests',)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tests=None,  # List of (query, expected_result) tuples
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []

    def execute(self, context):
        self.log.info("Connecting to Redshift")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.tests:
            raise ValueError("No data quality tests were provided.")

        for i, (query, expected_result) in enumerate(self.tests):
            self.log.info(f"Running data quality check {i+1}: {query}")
            records = redshift_hook.get_records(query)

            if not records or not records[0]:
                raise ValueError(f"Test {i+1} failed: No records returned for query:\n{query}")

            actual_result = records[0][0]
            if actual_result != expected_result:
                raise ValueError(
                    f"Test {i+1} failed: Query returned {actual_result}, expected {expected_result}\nQuery: {query}"
                )

            self.log.info(f"Test {i+1} passed. Got {actual_result} as expected.")

        self.log.info("All data quality checks passed ")
