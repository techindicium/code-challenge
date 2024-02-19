from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from seu_script import main

# Defina os argumentos padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Crie a DAG
dag = DAG(
    'processamento_dados',
    default_args=default_args,
    description='DAG para processamento de dados',
    schedule_interval='@daily',
)

# Defina a função para executar o script Python principal
def run_python_script(*args, **kwargs):
    main()

# Crie uma tarefa para executar o script Python principal
executar_processamento = PythonOperator(
    task_id='executar_processamento',
    python_callable=run_python_script,
    dag=dag,
)

# Crie uma tarefa para verificar a existência do arquivo
verificar_existencia_arquivo = PythonOperator(
    task_id='verificar_existencia_arquivo',
    python_callable=main.verificar_existencia_arquivo,
    provide_context=True,
    on_success_callback=[main.enviar_email_sucesso],
    on_failure_callback=[main.enviar_email_falha],
    dag=dag,
)

# Defina as dependências da DAG
verificar_existencia_arquivo >> executar_processamento
