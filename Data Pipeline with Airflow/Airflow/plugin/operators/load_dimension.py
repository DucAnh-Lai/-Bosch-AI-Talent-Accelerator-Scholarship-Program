from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    DAG operator used to populate dimension tables  in a STAR schema.
    :param string  redshift_conn_id: Redshift database location
    :param string  table: Table in Redshift database
    :param string  insert_sql: SQL to extract songplays data
    :param string  delete_table: 'Y' delete table if existed before load; 'N': only append table
    
    """

    ui_color = '#80BD9E'

        
    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """
        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 insert_sql,
                 delete_table,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = insert_sql
        self.delete_table = delete_table

    def execute(self, context):
        self.log.info("LoadDimensionOperator begin execute")
        # connect to Redshift
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f"Connecting to Redshift...")
              
        #Choose delete before add table or directly add table
        if self.delete_load:
            self.log.info("Delete Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.insert_sql
        )
        self.log.info(f"Executing {formatted_sql} ...")
        redshift.run(formatted_sql)