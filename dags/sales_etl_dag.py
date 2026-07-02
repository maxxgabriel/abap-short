"""
Airflow DAG for Sales ETL process.
Orchestrates Extract -> Transform -> Load workflow with PySpark.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import yaml
import logging


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['etl-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# DAG definition
dag = DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='Sales Data ETL Pipeline - Extract, Transform, Load',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['sales', 'etl', 'analytics'],
)


def load_config():
    """Load configuration from YAML file."""
    with open('/opt/airflow/config/config.yaml', 'r') as f:
        return yaml.safe_load(f)


def initialize_etl(**context):
    """
    Initialize ETL process.
    Maps to ABAP gc_step.init.
    """
    logging.info("Initializing ETL process")
    
    # Load configuration
    config = load_config()
    
    # Generate ETL run ID
    etl_run_id = f"ETL{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Push to XCom for downstream tasks
    context['task_instance'].xcom_push(key='etl_run_id', value=etl_run_id)
    context['task_instance'].xcom_push(key='config', value=config)
    
    logging.info(f"ETL Run ID: {etl_run_id}")
    logging.info("ETL process initialized successfully")


def validate_prerequisites(**context):
    """
    Validate prerequisites before ETL execution.
    Maps to ABAP gc_step.validate.
    """
    logging.info("Validating ETL prerequisites")
    
    config = context['task_instance'].xcom_pull(key='config', task_ids='init_task')
    
    # Validate configuration
    required_keys = ['extract', 'transform', 'load', 'business_rules']
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required configuration key: {key}")
    
    logging.info("Prerequisites validated successfully")


def extract_sales_data(**context):
    """
    Extract sales data from source.
    Maps to ABAP gc_step.extract.
    """
    logging.info("Starting data extraction")
    
    # This would typically be a SparkSubmitOperator
    # For demonstration, showing the setup
    execution_date = context['execution_date']
    from_date = (execution_date - timedelta(days=7)).strftime('%Y-%m-%d')
    to_date = execution_date.strftime('%Y-%m-%d')
    
    context['task_instance'].xcom_push(key='from_date', value=from_date)
    context['task_instance'].xcom_push(key='to_date', value=to_date)
    
    logging.info(f"Extraction configured for date range: {from_date} to {to_date}")


def transform_sales_data(**context):
    """
    Transform sales data with business logic.
    Maps to ABAP gc_step.transform.
    """
    logging.info("Starting data transformation")
    
    etl_run_id = context['task_instance'].xcom_pull(key='etl_run_id', task_ids='init_task')
    
    # Transform logic would be in Spark job
    logging.info(f"Transformation using ETL Run ID: {etl_run_id}")


def load_analytics_data(**context):
    """
    Load transformed data to target.
    Maps to ABAP gc_step.load.
    """
    logging.info("Starting data load")
    
    # Load logic would be in Spark job
    logging.info("Data loaded successfully")


def complete_etl(**context):
    """
    Complete ETL process and log summary.
    Maps to ABAP gc_step.complete.
    """
    logging.info("Completing ETL process")
    
    etl_run_id = context['task_instance'].xcom_pull(key='etl_run_id', task_ids='init_task')
    
    # Log summary
    logging.info(f"ETL Run ID {etl_run_id} completed successfully")


def handle_etl_error(**context):
    """
    Handle ETL errors and cleanup.
    Maps to ABAP gc_step.error.
    """
    logging.error("ETL process encountered errors")
    
    # Cleanup logic
    logging.info("Error handling completed")


# Task definitions
init_task = PythonOperator(
    task_id='init_task',
    python_callable=initialize_etl,
    provide_context=True,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_task',
    python_callable=validate_prerequisites,
    provide_context=True,
    dag=dag,
)

extract_task = SparkSubmitOperator(
    task_id='extract_task',
    application='/opt/airflow/jobs/extract_job.py',
    name='sales_extract',
    conf={
        'spark.executor.memory': '2g',
        'spark.driver.memory': '1g',
        'spark.executor.instances': '2',
    },
    application_args=[
        '--from_date', '{{ task_instance.xcom_pull(key="from_date", task_ids="extract_setup") }}',
        '--to_date', '{{ task_instance.xcom_pull(key="to_date", task_ids="extract_setup") }}',
        '--config', '/opt/airflow/config/config.yaml',
    ],
    dag=dag,
)

extract_setup = PythonOperator(
    task_id='extract_setup',
    python_callable=extract_sales_data,
    provide_context=True,
    dag=dag,
)

transform_task = SparkSubmitOperator(
    task_id='transform_task',
    application='/opt/airflow/jobs/transform_job.py',
    name='sales_transform',
    conf={
        'spark.executor.memory': '4g',
        'spark.driver.memory': '2g',
        'spark.executor.instances': '4',
    },
    application_args=[
        '--etl_run_id', '{{ task_instance.xcom_pull(key="etl_run_id", task_ids="init_task") }}',
        '--config', '/opt/airflow/config/config.yaml',
        '--input_path', '/tmp/extracted_data',
    ],
    dag=dag,
)

load_task = SparkSubmitOperator(
    task_id='load_task',
    application='/opt/airflow/jobs/load_job.py',
    name='sales_load',
    conf={
        'spark.executor.memory': '2g',
        'spark.driver.memory': '1g',
        'spark.executor.instances': '2',
    },
    application_args=[
        '--config', '/opt/airflow/config/config.yaml',
        '--input_path', '/tmp/transformed_data',
    ],
    dag=dag,
)

complete_task = PythonOperator(
    task_id='complete_task',
    python_callable=complete_etl,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)

error_task = PythonOperator(
    task_id='error_task',
    python_callable=handle_etl_error,
    provide_context=True,
    trigger_rule='one_failed',
    dag=dag,
)

# Task dependencies - mapping ABAP orchestrator flow
init_task >> validate_task >> extract_setup >> extract_task >> transform_task >> load_task >> complete_task
[extract_task, transform_task, load_task] >> error_task