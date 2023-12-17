from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from client import hit_nasa_api


default_args = {
    'owner': 'arima',
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_warehouse_dag',
    default_args=default_args,
    description='DAG for loading local warehouse',
    schedule_interval=timedelta(days=7),  # Set the interval to run daily
)

# Python function to hit API and store data
def hit_api_and_store_dag(**kwargs):
    start_date = kwargs['ds']
    end_date = (datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=6)).strftime('%Y-%m-%d')
    output_file = f"/path/to/store/data/{start_date}_{end_date}_data.csv"
    
    hit_nasa_api(start_date, end_date)

# Operator to execute Python function
api_data_collector_task = PythonOperator(
    task_id='hit_api_and_store',
    python_callable=hit_api_and_store_dag,
    provide_context=True,  # Pass the context to the PythonOperator
    dag=dag,
)

# Set task dependencies (if any)
# ...

# Define the DAG structure
api_data_collector_task
