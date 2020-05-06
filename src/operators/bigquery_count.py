from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.bigquery_hook import BigQueryHook


class BigQueryCountOperator(BaseOperator):

    @apply_defaults
    def __init__(self, dataset, table, conn_id=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id or "bigquery_default"
        self.sql = """
SELECT row_count
FROM {}.__TABLES__
WHERE table_id = "{}"
""".format(dataset, table)

    def execute(self, context=None):
        self.log.info('Executing SQL statement: ' + self.sql)
        cursor = self.get_db_hook()
        cursor.execute(self.sql)
        records = cursor.fetchone()
        self.log.info("Record: " + str(records[0]))
        records_int = int(records[0])
        return records_int

    def get_db_hook(self):
        return BigQueryHook(bigquery_conn_id=self.conn_id).get_conn().cursor()
