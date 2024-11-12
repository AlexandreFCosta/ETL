import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

#

# Adiciona o caminho do diretório onde `extract_data.py` está localizado
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Importa o módulo `main` de `extract_data.py`, que executa a lógica principal
from jobs.python.bronze.extract_data import main as extract_data_main
from jobs.python.gold.load_data import main as run_data_aggregation


# Função de notificação para Slack
def send_failure_slack_message(context):
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = context.get('execution_date')
    slack_message = f"🚨 Task {task_id} failed in DAG {dag_id}. Execution Date: {execution_date}"

    slack_operator = SlackAPIPostOperator(
        task_id='slack_notification',
        slack_conn_id='slack_conn',
        channel='C08133R85G8',
        text=slack_message
    )

    slack_operator.execute(context=context)

# Função para executar o script de qualidade de dados
def run_data_quality_checks():
    # Caminho do arquivo Python de qualidade de dados
    script_path = "/opt/airflow/data_quality/data_quality_checks.py"
    
    # Executa o script Python como um subprocesso
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    
    # Checa se houve erros na execução do script
    if result.returncode != 0:
        raise Exception(f"Data Quality Check Failed: {result.stderr}")
    print(result.stdout)

# Função para executar o script de transformação de dados
def run_data_transformation():
    # Caminho do arquivo Python de transformação de dados
    script_path = "/opt/airflow/silver/transform_data.py"
    
    # Executa o script Python como um subprocesso
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    
    # Checa se houve erros na execução do script
    if result.returncode != 0:
        raise Exception(f"Data Transformation Failed: {result.stderr}")
    print(result.stdout)

# Define as funções para as tarefas
def start_task():
    print("Jobs started")

def end_task():
    print("Jobs completed successfully")

# Definição da DAG
with DAG(
    "sparking_flow",
    description="Pipeline de dados com checagem de qualidade e agregação",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:   

    # Define os operadores para as tarefas da DAG
    start = PythonOperator(
        task_id='start',
        python_callable=start_task,
        dag=dag,
    )

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_main,
        on_failure_callback=send_failure_slack_message,  # Chama a função main do arquivo extract_data.py
        dag=dag,
    )

    data_quality_checks = PythonOperator(
        task_id='data_quality_checks',
        python_callable=run_data_quality_checks,
        on_failure_callback=send_failure_slack_message,  # Chama a função de qualidade de dados
        dag=dag,
    )

    data_transformation = PythonOperator(
        task_id='data_transformation',
        python_callable=run_data_transformation,
        on_failure_callback=send_failure_slack_message,  # Chama o script de transformação de dados
        dag=dag,
    )

    data_aggregation = PythonOperator(
        task_id='data_aggregation',
        python_callable=run_data_aggregation,
        on_failure_callback=send_failure_slack_message,  # Chama o script de agregação de dados
        dag=dag,
    )

    end = PythonOperator(
        task_id='end',
        python_callable=end_task,
        on_failure_callback=send_failure_slack_message,
        dag=dag,
    )

    # Define a ordem das tarefas
    start >> extract_data >> data_quality_checks >> data_transformation >> data_aggregation >> end
