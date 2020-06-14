from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Operator to perform validation of the results of a Data Pipeline.
    
    :param redshift_conn_id: Airflow connection id to Redshift configuration (default '')
    :param table: Target Redshift table to validate (default '')
    :param test_sql: SSql to retrive the data to be validated. Supports Airflow Macros templating (default '')
    :param result_checker: Lambda callback function to check if the result is valid. If false is return the operator will raise a value error  (default 'lambda x: True')
    """
    ui_color = '#89DA59'
    template_fields = ("test_sql",)

    @apply_defaults
    def __init__(self,
                redshift_conn_id = "",
                table="",
                test_sql="",
                result_checker=(lambda x: True),
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.test_sql = test_sql
        self.result_checker = result_checker

    def execute(self, context):
        self.log.info(f"Started data quality verification on Redshift table ({self.table})")
        sql = self.test_sql.format(**context)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        result = redshift_hook.get_records(sql)
        if self.result_checker(result[0][0]) == False:
            raise ValueError(f"Data quality check failed against {self.table}.")

        self.log.info(f"Completed data quality verification on Redshift table ({self.table}) successfully")