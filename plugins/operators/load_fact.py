from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Operator to perform loading of data into of the fact tables in a Data Pipeline.
    It appends new rows on every run on the assumption for the fact data is partitioned on every run.
    
    :param redshift_conn_id: Airflow connection id to Redshift configuration (default '')
    :param table: Target Redshift table to load the data (default '')
    :param select_sql: Sql to retrive the data from the source to be loaded. Supports Airflow Macros templating (default '')  
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = "",
                table="",
                select_sql="",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):
        self.log.info(f"Started data insert into destination Redshift fact table ({self.table})")      
        sql = f"""
            INSERT INTO {self.table}
            {self.select_sql}
        """

        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(sql, True)
        self.log.info(f"Completed data insert into destination Redshift fact table ({self.table}) successfully")
        
