from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    DAG operator for loading data
    
    """
    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table,
                 insert_sql,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = insert_sql

    def execute(self, context):
        self.log.info("LoadFactOperator is implementing...")
        
        #Connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Connecting to Redshift...")
        
        #Build insert statement
        sql = LoadFactOperator.insert_sql.format(
                        self.table,
                        self.insert_sql)
        self.log.info(f"insert sql: {sql}")

        redshift_hook.run(sql_stmt)
        self.log.info(f"LoadFactOperator completed with the table {self.table_name}")
