import os

import jpype
from airflow.exceptions import AirflowException


from airflow.providers.jdbc.hooks.jdbc import jaydebeapi
from airflow.logging_config import log as logger
from airflow.models.variable import Variable as v
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
#from airflow.providers.mysql.operators.mysql import MySqlOperator
#from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#from airflow.providers.mysql.hooks.mysql import MySqlHook
from db import DB


# noinspection PyUnresolvedReferences
class Palantir(DB):
    def __init__(self) -> None:
        self.palantir_jars = [v.get("palantir"), v.get("palantir_extra")]
        self.truststore = v.get("palantir_truststore")
        self.token = v.get("palantir_token")
        self.uri = v.get("palantir_uri").format(self.token, self.truststore)
        self.conn = None
        self.cursor = None
    
    def query(self, sql):
        """
        run palantir sql query and returns data in format of dataframe
        """
        return self.fetch_query_as_dataframe(sql=sql)

    def connect(self):
        jpype.startJVM(jpype.getDefaultJVMPath(), "-ea", "-Djava.class.path={}".format(os.pathsep.join(self.palantir_jars)))
        self.conn = jaydebeapi.connect('com.palantir.foundry.sql.jdbc.FoundryJdbcDriver', url=self.uri)
        logger.info('Palantir Connection is {}'.format(self.conn))
        self.cursor = self.conn.cursor()
        logger.info('Palantir Cursor is {}'.format(self.cursor))
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            jpype.shutdownJVM()
        except Exception as exc:
            logger.warning('Exception raised while shutting down JVM, exception is: %s', exc)


#class MysqlDb(DB):
#    def __init__(self, conn_id) -> None:
#        self.hook = MySqlHook(conn_id)
#        self.conn = self.hook.get_conn()
#        self.cursor = self.conn.cursor()


#class MssqlDb(DB):
#    def __init__(self, conn_id) -> None:
#        self.hook = MsSqlHook(conn_id)
#        self.conn = self.hook.get_conn()
#        self.cursor = self.conn.cursor()


#class XcomMySqlOperator(MySqlOperator):
#    """
#    derive from MySqlOperator and added Xcom to it.
#    """
#    def execute(self, context):
#        self.log.info('Executing: %s', self.sql)
#        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id,
#                         schema=self.database)
#        return hook.get_pandas_df(
#          self.sql,
#          parameters=self.parameters
 #       )
