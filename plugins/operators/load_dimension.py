from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Operator to perform loading of data into of the dimension tables in a Data Pipeline.
    It supports both upserting and full load
        - upserting: is the appending or updating of new records for tables that are usually sourced from staging data that is partitioned and not fully loaded on every run.
        - full load: truncating and loading the table in full
    Reference implementation: https://github.com/awsdocs/amazon-redshift-developer-guide/blob/master/doc_source/merge-replacing-existing-rows.md

    
    :param redshift_conn_id: Airflow connection id to Redshift configuration (default '')
    :param table: Target Redshift table to load the data (default '')
    :param primary_key: Primary key of Redshift table this is important to the upserting process to remove duplicates (default '')
    :param select_sql: Sql to retrive the data from the source to be loaded. Supports Airflow Macros templating (default '')
    :param append_insert: indicates if this is used in upserting mode (default True)
    """    

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = "",
                table="",
                primary_key="",
                select_sql="",
                upsert=True,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.primary_key = primary_key
        self.select_sql = select_sql
        self.upsert = upsert

    def execute(self, context):
        self.log.info(f"Started data { 'upset' if self.upsert else 'reload' } into destination Redshift dimension table ({self.table})")
        if self.upsert:
            stage_table = f"#stage_{self.table}"
            
            sql = f"""
                CREATE TEMP TABLE {stage_table} (LIKE {self.table});

                INSERT INTO {stage_table}
                {self.select_sql};

                BEGIN TRANSACTION;
                
                DELETE FROM {self.table}
                USING {stage_table}
                WHERE {self.table}.{self.primary_key} = {stage_table}.{self.primary_key};

                INSERT INTO {self.table}
                SELECT * FROM {stage_table};

                END TRANSACTION;

                DROP TABLE {stage_table};
            """
        else:
            sql = f"""
                TRUNCATE TABLE {self.table};
                
                INSERT INTO {self.table}
                {self.select_sql};
            """

        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(sql, True)
        self.log.info(f"Completed data { 'upset' if self.upsert else 'reload' } into destination Redshift dimension table ({self.table}) successfully.")
        