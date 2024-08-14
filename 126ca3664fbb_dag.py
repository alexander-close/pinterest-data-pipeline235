from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


# specific job (Databricks notebook file)
job_id = '448661580670850'

# Default arguments for the DAG
default_args = {
    'owner': 'Alexander Close',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    '126ca3664fbb_dag', # the name of the DAG
    default_args=default_args,
    description='A DAG to trigger a specific Databricks notebook daily',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['pinterest', 'notebook'],
)

# task to trigger the notebook job
trigger_notebook_task = DatabricksRunNowOperator(
    task_id='run_pinterest_notebook',
    databricks_conn_id='databricks_default',  # connection to Databricks (configured in Airflow)
    job_id=job_id,  # ID of the Databricks job
    notebook_params={  # optional
        # 'param1': 'value1',
        # 'param2': 'value2',
    },
    dag=dag,
)

# set the task in the DAG
trigger_notebook_task