from airflow import DAG
from airflow.operators.python import PythonOperator


from airflow.operators.email import EmailOperator
from airflow.utils.file import mkdirs
from datetime import datetime,timedelta

import sys
sys.path.append('/opt/airflow/dags/repo/dags')
from conn_db import Palantir
from pathlib import Path
from db import MysqlOps
from airflow.logging_config import log as logger
from airflow.exceptions import AirflowException
import pandas as pd
import numpy as np
import DICT_PS_CUSTOM_PRODUCT_FOR_SSOC
from email import email_on_customization
import tempfile
import boto3

default_args = {
    "owner": "Karen Huang",
    "start_date": datetime(2025, 1, 1),
    "email": ["karen.huang@merckgroup.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=20),
}


dag = DAG(
    dag_id='dag_SSOC_ps_custom_product_daily',
    default_args=default_args,
    schedule_interval='00 05 * * *',
    catchup=False,
)


def conn_palantir(**kwargs):
    ti = kwargs['ti']
    xcom_key = kwargs['xcom_key']
    save_path = kwargs['save_path']
    data = kwargs['palantir_data_path']
    sql = 'select * from `{0}`'.format(data)
    with Palantir() as palantir:
        df_palantir = palantir.query(sql)
        logger.info('In total {0} rows retrived from {1}'.format(len(df_palantir), xcom_key))

        df_palantir.to_csv(save_path, sep='|', index=False)
        logger.info('File saved at {0}, {1} rows in total'.format(save_path, len(df_palantir)))
        
    value = {'save_path': save_path, 'row_count': len(df_palantir)}
    ti.xcom_push(key=xcom_key, value=value)


def conn_mysql(**kwargs):
    ti = kwargs['ti']
    xcom_key = kwargs['xcom_key']
    conn_id = kwargs['conn_id']
    table_name = kwargs['mysql_table_name']
    file_path = ti.xcom_pull(key=xcom_key)['save_path']

    mysql_db = MysqlOps(conn_id)
    mysql_db.truncate_table(table_name)

    # df_palantir = pd.read_csv(file_path, sep='|')
    # df_palantir = df_palantir.replace({np.nan:None})

    # strainsDB_db.insert_with_dataframe2(df_palantir, table_name)
    mysql_db.bulk_load_with_csv(file_path, table_name, "|")
    logger.info('{0} {1} fully inserted'.format(conn_id, table_name))


def email_on_success(**kwargs):
    ti = kwargs['ti']
    xcom_key = kwargs['xcom_key']
    row_count = ti.xcom_pull(key=xcom_key)['row_count']
    dag = kwargs.get('dag_run')
    content = "{0} records successfully inserted into {1}".format(row_count, xcom_key)
    # email_op = EmailOperator(
    #     task_id = "send_email_on_success",
    #     to = ["karen.huang@merckgroup.com"],
    #     subject = "Airflow success: {0}".format(kwargs.get('dag_run')),
    #     html_content = "{0} records successfully inserted into {1}".format(row_count, xcom_key)
    # )
    # email_op.execute(kwargs)
    email_on_customization(
        send_to= ["karen.huang@merckgroup.com"],
        content = content,
        dag = dag
    )


pull_ps_custom_product_from_palantir_task = PythonOperator(
    task_id = 'pull_ps_custom_product_from_palantir',
    python_callable = conn_palantir,
    op_kwargs =  {
        "xcom_key": "ps_custom_product",
        "palantir_data_path": DICT_PS_CUSTOM_PRODUCT_FOR_SSOC['from'],
        "save_path": "ps_custom_product.csv",
    },
    dag=dag,
    provide_context=True
)

truncate_table_and_load_csv_to_mysql_task = PythonOperator(
    task_id = 'truncate_table_and_load_csv_to_mysql',
    python_callable = conn_mysql,
    op_kwargs = {
        "xcom_key": "ps_custom_product",
        "conn_id": "SSOC_dev",
        "mysql_table_name": DICT_PS_CUSTOM_PRODUCT_FOR_SSOC['to']
    },
    dag = dag,
    provide_context = True
)


send_mail_task = PythonOperator(
    task_id = "email_notification_on_success",
    python_callable = email_on_success,
    op_kwargs = {
        "xcom_key": "ps_custom_product",
    },
    dag = dag,
    provide_context = True
)


pull_ps_custom_product_from_palantir_task >> truncate_table_and_load_csv_to_mysql_task >> send_mail_task

