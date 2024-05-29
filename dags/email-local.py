from datetime import datetime
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from smtplib import SMTP
from airflow.operators.python import get_current_context

def print_hello():
    return 'Hello world!'

def my_email_func():
    context = get_current_context()
    smtp = SMTP()
    smtp.set_debuglevel(10)
    smtpServer = context["params"]["smtp_server"]
    smtpPort = context["params"]["smtp_port"]
    smtpUser = ""
    smtpPass = ""
    from_addr = context["params"]["from_address"]
    to_addr   = context["params"]["to_address"]
    
    smtp.connect(smtpServer, smtpPort)
    #smtp.login(smtpUser, smtpPass)

    #from_addr = "Sender Name <info@example.com>"
    #to_addr = "recipient@example.com"

    subj = "hello"
    date = datetime.now().strftime("%d/%m/%Y %H:%M")

    message_text = "Hello\nThis is a mail from your server\n\nBye\n"

    msg = "From: %s\nTo: %s\nSubject: %s\nDate: %s\n\n%s" % (from_addr, to_addr, subj, date, message_text)

    smtp.sendmail(from_addr, to_addr, msg)
    smtp.quit()
    return 'Email sent!'


default_args = {
        'owner': 'abe',
        'start_date':datetime(2024, 5, 26)
}

dag = DAG('send_email_test',
          description='SMTP Function DAG',
          schedule_interval=None,
          default_args=default_args, catchup=False,
          params={
            "smtp_server": Param(
                "smtp-server",
                type="string",
                description="username",
            ),
            "smtp_port": Param(
                "587", type="string", description="SMTP Server Port "
            ),
            "smtp_user": Param(
                "smtp-username", type=["string"], description="SMTP Username",
            ),
            "smtp_password": Param(
                "smtp-password", type=["string"], description="SMTP Password",
            ),
            "from_address": Param(
                "from@smtp.com", type=["string"], description="SMTP From Server",
            ),
            "to_address": Param(
                "to@smtp.com", type=["string"], description="SMTP To Server",
            )
         },
    access_control={"All": {"can_read", "can_edit", "can_delete"}},
    )

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

email = PythonOperator(task_id='email_task', python_callable=my_email_func, dag=dag)

email >> dummy_operator >> hello_operator
