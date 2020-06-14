from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Operator to perform loading of stagin data from S3 to Redshift in a Data Pipeline.
    
    :param redshift_conn_id: Airflow connection id to Redshift configuration (default '')
    :param aws_credentials_id: Airflow connection id to AWS S3 configuration (default '')
    :param table: Target Redshift table to load data (default '')
    :param s3_bucket: 3 bucket name of source data (default '')
    :param s3_key: Key of S3 object to be loaded. Supports Airflow Macros templating and file globbing  (default '')
    :param s3_region: Region of AWS instance (default '')
    :param json_format: A JSON schema file or 'auto', for reading the source data (default '')
    """

    ui_color = "#358140"
    template_fields = ("s3_key",)
    stage_sql = """
            COPY {} 
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION AS '{}'
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS JSON '{}';
        """      

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id = "",
        aws_credentials_id = "",
        table="",
        s3_bucket="",
        s3_key="",
        s3_region="",
        json_format="auto",      
        *args, **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.json_format = json_format

    def execute(self, context):
        self.log.info(f"Started staging to destination Redshift table ({self.table})")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        sql = StageToRedshiftOperator.stage_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.s3_region,
            self.json_format
        )
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info(f"Clearing data from destination Redshift table ({self.table})")
        redshift_hook.run(f"TRUNCATE {self.table}")      
        self.log.info("Copying data from S3 to Redshift")        
        redshift_hook.run(sql)
        self.log.info(f"Completed staging to destination Redshift table ({self.table}) successfully")
