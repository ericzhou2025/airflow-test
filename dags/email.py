import os
from airflow.operators.email import EmailOperator


def email_on_customization(**kwargs):
    '''
    argument list:
    send_to: recepients list
    dag: DAG name
    content: email body in HTML format
    '''
    send_to = kwargs['send_to']
    dag = kwargs['dag']
    content = kwargs['content']
    host = os.getenv("HOSTNAME")
    email_op = EmailOperator(
        task_id = "send_email_on_success",
        to = send_to,
        subject = f"[{host}] Airflow DAG in success: {dag}",
        html_content = content
    )
    email_op.execute(kwargs)
