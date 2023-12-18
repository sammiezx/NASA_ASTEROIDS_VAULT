from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from dotenv import load_dotenv
import yaml

from client import hit_nasa_api
from adapter import parse_response
from connector.cassandra_connector import get_session, create_and_set_keyspace, create_table, save_dataframe_to_cassandra

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
    schedule_interval=timedelta(days=7)
)

load_dotenv()
with open(os.getenv("CONF_PATH"), 'r') as file:
    conf = yaml.load(file, Loader=yaml.FullLoader)
session = get_session()
create_and_set_keyspace(session, conf['cassandra_keyspace_name'])
create_table(session, conf['cassandra_table_name'])

def clear_buffer():
    file_path = conf['buffer_path']
    try:
        os.remove(file_path)
        print(f"File '{file_path}' has been deleted.")
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


def get_payload(**kwargs):
    current_date = datetime.now().date()
    end_date = current_date.strftime('%Y-%m-%d')
    start_date = (current_date - timedelta(days=7)).strftime('%Y-%m-%d')
    try:
        df = hit_nasa_api(start_date, end_date)
        df.to_csv(conf['buffer_path'], index=False)
        return
    except Exception as e:
        raise e

def preprocess_payload(**kwargs):
    try:
        df = pd.read_csv(conf['buffer_path'])
        df = parse_response(df)
        # adapter takes care of most of the parsing and all, while more can be added in this task later on
        clear_buffer()
        df.to_csv(conf['buffer_path'], index=False)
    except Exception as e:
        raise e

def dump_payload(**kwargs):
    try:
        df = pd.read_csv(conf['buffer_path'])
        save_dataframe_to_cassandra(session, df, conf['cassandra_table_name'])
        clear_buffer()
    except Exception as e:
        raise e


get_payload_task = PythonOperator(
    task_id='get_payload_task',
    python_callable=get_payload,
    provide_context=True, 
    dag=dag,
)

preprocess_payload_task = PythonOperator(
    task_id='preprocess_payload_task',
    python_callable=preprocess_payload,
    provide_context=True, 
    dag=dag,
)

dump_payload_task = PythonOperator(
    task_id='dump_payload_task',
    python_callable=dump_payload,
    provide_context=True, 
    dag=dag,
)

get_payload_task >> preprocess_payload_task >> dump_payload_task