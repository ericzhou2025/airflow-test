import typing as t

import pandas as pd
import pandas.io.sql as sqlio
from airflow.exceptions import AirflowException
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.logging_config import log as logger
from db_utils import get_tuples, get_columns_in_list

DB_PYTEST = {
    "Host": "localhost",
    "Username": "usr_ten1",
    "Password": "usr_ten1",
    "Database": "aidd_molecule",
    "Port": 5432
}

DB_LOCAL = {
    "Host": "host.docker.internal",
    "Username": "usr_ten1",
    "Password": "usr_ten1",
    "Database": "aidd_molecule",
    "Port": 5432
}


class MysqlOps:
    def __init__(self, conn_id) -> None:
        self.hook = MySqlHook(conn_id, local_infile=True)

    def fetch_query_as_dataframe(self, query: str) -> pd.DataFrame:
        try:
            return self.hook.get_pandas_df(query)
        except AirflowException as error:
            logger.error("Error retrieving data from query: %s", error)

    def write_to_db(self, sql, param: t.Union[t.List, t.Tuple, t.Dict]) -> None:
        conn = self.hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql, param)
            conn.commit()
        except AirflowException as err:
            logger.error("exception happened when insert data: ", err)
            conn.rollback()
        finally:
            conn.close()

    def truncate_table(self, table_name) -> None:
        conn = self.hook.get_conn()
        try:
            sql = f'truncate table {table_name}'
            with conn.cursor() as cursor:
                cursor.execute(sql)
            conn.commit()
        except AirflowException as err:
            logger.error("exception happened when truncating data: ", err)
            conn.rollback()
        finally:
            conn.close()

    def insert_with_dataframe(self, df: pd.DataFrame, table_name:str) -> None:
        """store records to table."""
        self.hook.insert_rows(
            table= table_name,
            rows= get_tuples(df),
            target_fields=get_columns_in_list(df),
            commit_every=df.shape[0]
        )

    # def insert_with_dataframe2(self, df: pd.DataFrame, table_name:str) -> None:
    #     keys = get_columns_in_list(df)
    #     values = get_tuples(df)
    #     key_sql = ','.join(keys)
    #     values_sql = ','.join(['%s']*df.shape[1])
    #     sql = """
    #             INSERT INTO %s (%s) VALUES (%s)

    #           """ % (table_name, key_sql, values_sql)

    #     self.write_to_db(sql, values)

    def bulk_load_with_csv(self, file_path: str, table: str, sep = ",") -> None:
        self.hook.bulk_load_custom(
            table=table,
            tmp_file= file_path,
            extra_options="FIELDS TERMINATED BY '{}' ENCLOSED BY '\"' IGNORE 1 lines".format(sep)
        )

class DB:
    def __init__(self, conn, cursor) -> None:
        try:
            self.conn = conn
            self.cursor = cursor
        except Exception as e:
            logger.error(e)
            logger.error("Unable to connect to DB. Error: %s" % e)

    def table_exists(self, table):
        sql_str = """
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_name = %s
                """ % table
        self.cursor.execute(sql_str)
        if self.cursor.fetchone([0]) == 1:
            return True
        return False

    def truncate_table(self, table):
        sql_str = """
                    TRUNCATE TABLE %s
                """ % table
        self.cursor.execute(sql_str)
        sql_str = """
                    SELECT COUNT(*) FROM %s
                """ % table
        self.cursor.execute(sql_str)
        if self.cursor.fetchone()[0] == 0:
            return True
        return False

    def fetch_query_as_dataframe(self, sql):
        result = sqlio.read_sql_query(sql, self.conn)
        return result

    def insert_dataframe_to_db(self, df, table, stable_field_list=None):
        is_inserted = False
        try:
            # remove columns with name contains "()",eg: BILLING_BLOCK(ITEM)
            odd_names = []
            for char in ['(',')','/','%','DIV']:
                odd_names.extend(list(df.filter(like=char).columns))
            df = df.drop(odd_names, axis=1)
            keys = [str(k) for k in df.keys()]
            values = df.values.tolist()
            key_sql = ','.join(keys)
            values_sql = ','.join(['%s']*df.shape[1])
            insert_data_str = """
                              INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE

                              """ % (table, key_sql, values_sql)
            if stable_field_list:
                keys = list(set(keys) - set(stable_field_list))
            update_str = ','.join([" {key} = VALUES({key})".format(key=key) for key in keys])
            insert_data_str += update_str
            print(insert_data_str)
            print(values[0])
            self.cursor.executemany(insert_data_str, values)
            self.conn.commit()
            logger.info('Successfully inserted %d records into %s' % (len(df), table))
            is_inserted = True
        except Exception as e:
            logger.error("Error inserting result into %s: %s" % (table, e))

        return is_inserted

    def update_is_deleted(self, table):
        is_deleted = False
        # turn "is_deleted" as True only if the update_date is not today
        try:
            update_str = """
                        UPDATE %s SET is_deleted =
                        CASE
                            WHEN updated_date = CAST( GETDATE() AS Date ) THEN
                            0
                            ELSE 1
                        END
                        """ % (table)
            self.cursor.execute(update_str)
            self.conn.commit()
            is_deleted = True
        except Exception as e:
            logger.error("Error reset is_deleted in table %s with error: %s" % (table, e))
        return is_deleted

    def close_connection(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception as e:
            logger.error("Unable to close connection. Error: %s" % e)

