import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.operators.email_operator import EmailOperator
from jobs.python.unity_tests.main_tests import run_tests
from jobs.python.bronze.extract_data import extract_data
from jobs.python.data_quality.data_quality import run_data_quality_checks
from datetime import datetime, timedelta

dag = DAG(
    dag_id="sparking_flow",
    default_args={
        "depends_on_past":False,
        "start_date": airflow.utils.dates.days_ago(1),
        "email": ["alexandrefcosta.dev@outlook.com"],
        "email_on_failure":True,
        "email_on_retry":True,
        "retries":1,
        "retry_delay": timedelta(seconds=5),
        "execution_timeout": timedelta(minutes=1),
        "owner": "Alexandre Costa",
    },
    schedule_interval="@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

unity_tests_run = PythonOperator(
    task_id="unity_tests_run",
    python_callable=run_tests,
    dag=dag
)

extract_data = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag
)

data_quality = PythonOperator(
    task_id="data_quality",
    python_callable=run_data_quality_checks,
    dag=dag
)

send_email = EmailOperator(
    task_id="send_email",
    to="<email de destino>",
    subject="Airflow Failure",
    html_content="""<h3>Falha no processo de ETL.</h3>""",
    dag=dag,
    trigger_rule="one_failed"
)

transform_data = PapermillOperator(
    task_id="transform_data",
    input_nb='/opt/airflow/jobs/python/silver/transform_data.ipynb',      
    output_nb='/opt/airflow/jobs/python/silver/transformed_data_output.ipynb', 
    dag=dag
)

load_data = PapermillOperator(
    task_id="load_data",
    input_nb='/opt/airflow/jobs/python/gold/load_data.ipynb', 
    output_nb='/opt/airflow/jobs/python/gold/load_data_output.ipynb',
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)


start >> unity_tests_run >> extract_data >> data_quality >> send_email
data_quality >> transform_data >> load_data >> end
