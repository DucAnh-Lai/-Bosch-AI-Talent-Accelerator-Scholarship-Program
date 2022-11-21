from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):

    ui_color = '#555FFF'
    create_table_sql_file = "/home/workspace/airflow/create_tables.sql"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('CreateTablesOperator execution ...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Creating tables ...")
        self.log.info("create_table_sql_file: {} ...".format(self.create_table_sql_file))

        with open(self.create_table_sql_file) as f:
            self.log.info("Opend!")
            sqls = f.read().strip().split(";")
            for sql in sqls:
                if len(sql.strip()) > 0:
                    self.log.info("sql {}".format(sql))
                    redshift.run(sql)

        self.log.info("Tables successfully created!")
        