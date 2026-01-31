from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Best Practice 1: Define default_args to handle failures automatically.
# This prevents you from waking up at 3 AM if a task fails once due to a network glitch.
default_args = {
    'owner': 'moi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,                        # Retry the task twice before marking as failed
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes between retries
}

# Best Practice 2: Separation of concerns.
# Ideally, these functions connect to external services (APIs, Databases).


# For now, we simulate data processing.


def extract_data():
    """Simulates extracting data from a source (e.g., API or CSV)."""
    print("Extracting data...")
    return "raw_data"


def transform_data(ti):
    """
    Simulates cleaning the data.
    'ti' is the Task Instance, used to pull data from the previous task (XComs).
    """
    raw_data = ti.xcom_pull(task_ids='extract_task')
    print(f"Transforming {raw_data}...")
    return "clean_data"


def load_data(ti):
    """Simulates loading data into a destination (e.g., Postgres, Snowflake)."""
    clean_data = ti.xcom_pull(task_ids='transform_task')
    print(f"Loading {clean_data} into the database.")


# Best Practice 3: Use the Context Manager (with DAG(...) as dag) for cleaner code.
with DAG(
    'mon_premier_dag_quotidien',
    default_args=default_args,
    description='A simple daily pipeline for upskilling',
    schedule_interval='@daily',       # Runs once a day at midnight
    start_date=datetime(2024, 1, 1),  # The date execution technically starts
    # CRITICAL: Prevents running all missed days since 2024 if you start today
    catchup=False,
    tags=['learning', 'daily'],
) as dag:

    # Task 1: Extraction
    t1 = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
    )

    # Task 2: Transformation
    t2 = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
    )

    # Task 3: Loading
    t3 = PythonOperator(
        task_id='load_task',
        python_callable=load_data,
    )

    # Best Practice 4: Define dependencies clearly at the end.
    # t1 runs first, then t2, then t3.
    t1 >> t2 >> t3
