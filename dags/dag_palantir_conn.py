import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from airflow.utils.dates import datetime, timedelta
from airflow.logging_config import log as logger
from airflow.exceptions import AirflowException
from airflow.utils.file import mkdirs
from airflow.models.variable import Variable as v
from pathlib import Path
from lib.conn_db import Palantir
import pandas as pd
from lib.cql.cypher_query import (
    cql_constraint,
    crunchbase_nodes_investor,
    crunchbase_nodes_company,
    crunchbase_nodes_category,
    crunchbase_rel_investor_vs_company,
    crunchbase_rel_company_vs_category,
    update_node_geocode
)

default_args = {
    "owner": "Stanley Yu",
    "depends_on_past": False,
    "start_date": datetime(2015,1,1),
    "email": ["stanley.yu@merckgroup.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def email_on_success(**context):
    email_op = EmailOperator(
        task_id = "send_email_on_",
        to= ["stanley.yu@merckgroup.com"],
        subject= "[Airflow] DAG in Success: {0}".format(
                        context.get('dag_run')
                ),
        html_content= 'job done',
        dag=dag
    )
    email_op.execute(context)

dag = DAG("test_palantir_conn", default_args=default_args, schedule_interval=None)

def conn_palantir(**kwargs):
    ti = kwargs["ti"]
    sql = kwargs["sql"]
    xcom_key = kwargs["xcom_key"]
    save_path = kwargs['save_path']
    try:
        palantir = Palantir()
        df_palantir = palantir.query(sql=sql)
        logger.info("In total {} rows retrieved from {}".format(len(df_palantir), xcom_key))
        # ti.xcom_push(key=xcom_key, value = df_palantir)

        if not Path(save_path).parents[0].exists():
            mkdirs(Path(save_path).parents[0], 777)
        df_palantir.to_csv(save_path, sep="|", index=False)
        logger.info("file saved at: %s, %s rows in total", save_path, len(df_palantir))
        ti.xcom_push(key = xcom_key, value = save_path)
    except AirflowException as err:
        logger.error(err)
        raise AirflowException(err.args[0])


def process_cb(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key = "cb_funding_rounds")
    neo4j = Neo4jHook(neo4j_conn_id="neo4j_test")

    neo4j.run(cql_constraint.format(node_label="investor", uniq_key="id"))
    neo4j.run(cql_constraint.format(node_label="company", uniq_key="id"))
    neo4j.run(cql_constraint.format(node_label="category", uniq_key="name"))

    neo4j_path = str(file_path).replace(v.get("airflow_export_path"),'')
    neo4j.run(crunchbase_nodes_investor.format(file_path=neo4j_path, field_sep="|"))
    neo4j.run(crunchbase_nodes_company.format(file_path=neo4j_path, field_sep="|"))

    try:
        df_cb_fr = pd.read_csv(file_path, sep="|")[["org_uuid","org_rank","category_list"]].dropna()
        df_cb_fr.drop_duplicates(inplace=True)

        df_cb_fr["category_list"] = df_cb_fr["category_list"].apply(lambda x: x.split(","))
        df_cb_fr = df_cb_fr.explode('category_list')
        f_company_vs_category = "palantir/test_org_vs_category.csv"
        df_cb_fr.to_csv(os.path.join(v.get("airflow_export_path") ,f_company_vs_category), sep="|", index=False)

        logger.info(pd.read_csv(os.path.join(v.get("airflow_export_path") ,f_company_vs_category), sep="|")[:10])

        neo4j.run(crunchbase_nodes_category.format(file_path=f_company_vs_category, field_sep="|"))

        neo4j.run(crunchbase_rel_investor_vs_company.format(file_path=neo4j_path, field_sep="|"))
        neo4j.run(crunchbase_rel_company_vs_category.format(file_path=f_company_vs_category, field_sep="|"))
    except ValueError as err:
        raise AirflowException(err)


def update_geocode(**kwargs):
    neo4j = Neo4jHook(neo4j_conn_id="neo4j_test")
    df = neo4j.run("match (i:investor) where not exists(i.longitude) return i")
    df_company = neo4j.run("match (i:company) where not exists(i.longitude) return i")
    df = df.append(df_company)
    for node in df["i"]:
        # print(node.identity, node.labels, node["country_code"])
        try:
            neo4j.run(
                update_node_geocode.format(
                node_label = node.labels,
                id = node.identity,
                addr = node["city"]+", "+node["region"]+ ", "+node["country_code"]
                )
            )
        except:
            continue


pull_cb_funding_rounds = PythonOperator(task_id='pull_cb_funding_rounds',
    python_callable= conn_palantir,
    op_kwargs={
        "sql":'''
            SELECT inv.investor_uuid, inv.investor_name, d_inv.rank as investor_rank,
                d_inv.country_code, d_inv.region, d_inv.city,d_inv.cb_url as investor_cb_url,
                fr.investment_type as funding_round_name, fr.raised_amount_usd,
                fr.announced_on as fund_date,
                fr.org_uuid, fr.org_name, org.cb_url as org_url, org.rank as org_rank,
                org.country_code as org_country_code, org.region as org_region, org.city as org_city,
                org.homepage_url as org_homepage_url,org.short_description as org_desc,
                org.category_groups_list, org.category_list
            FROM `/Life Science/ls-ontology-crunchbase/ontology/v4/cb_investments_v4` as inv
            JOIN `/Life Science/ls-ontology-crunchbase/ontology/v4/cb_investors_v4` as d_inv
            ON inv.investor_uuid = d_inv.uuid
            JOIN `/Life Science/ls-ontology-crunchbase/ontology/v4/cb_funding_rounds_v4` as fr
            ON inv.funding_round_uuid = fr.uuid
            JOIN `/Life Science/ls-ontology-crunchbase/ontology/v4/cb_organizations_v4` as org
            ON fr.org_uuid = org.uuid
            LIMIT 9999
            ''',
        "xcom_key": "cb_funding_rounds",
        "save_path": "/export/palantir/test_cb.csv",
    },
    dag=dag,
    provide_context=True,
)

process_csv_and_load_into_neo4j = PythonOperator(
    task_id = "process_csv_and_load_into_neo4j",
    python_callable=process_cb,
    # op_kwargs={
    #     "save_path":"/usr/local/airflow/export/palantir/test.csv",
    # },
    dag=dag,
    provide_context=True,
)

post_loading_update_geocode = PythonOperator(
    task_id = "post_loading_update_geocode",
    python_callable=update_geocode,
    dag=dag,
    provide_context=True,
)

send_email = PythonOperator(
    task_id = "email_notification_on_success",
    python_callable=email_on_success,
    dag=dag,
    provide_context=True
)

pull_cb_funding_rounds >> process_csv_and_load_into_neo4j >> post_loading_update_geocode >> send_email
