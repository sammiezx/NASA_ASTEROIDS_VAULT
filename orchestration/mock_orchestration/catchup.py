from datetime import datetime, timedelta
import pandas as pd
import os
from dotenv import load_dotenv
import yaml

from client import hit_nasa_api
from adapter import parse_response
from connector.cassandra_connector import get_session, create_and_set_keyspace, create_table, save_dataframe_to_cassandra, get_min_max_dates


load_dotenv()
with open(os.getenv("CONF_PATH"), 'r') as file:
    conf = yaml.load(file, Loader=yaml.FullLoader)
session = get_session()
create_and_set_keyspace(session, conf['cassandra_keyspace_name'])
create_table(session, conf['cassandra_table_name'])

date_offset, _ = get_min_max_dates(session)

def clear_buffer():
    file_path = conf['catchup_buffer_path']
    try:
        os.remove(file_path)
        print(f"File '{file_path}' has been deleted.")
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


def get_payload(date_offset, conf):
    if(date_offset < datetime.strptime(conf['catch_upto'], "%Y-%m-%d")):
        raise Exception("CAUGHT UP!!, [you can disable the dag now]")
    
    end_date = date_offset.strftime('%Y-%m-%d')
    start_date = (date_offset - timedelta(days=6)).strftime('%Y-%m-%d')
    date_offset = date_offset - timedelta(days=6)
    # print(start_date, end_date)

    try:
        df = hit_nasa_api(start_date, end_date)
        df.to_csv(conf['catchup_buffer_path'], index=False)
        return
    except Exception as e:
        raise e

def preprocess_payload():
    try:
        df = pd.read_csv(conf['catchup_buffer_path'])
        df = parse_response(df)
        # adapter takes care of most of the parsing and all, while more can be added in this task later on
        clear_buffer()
        df.to_csv(conf['catchup_buffer_path'], index=False)
    except Exception as e:
        raise e

def dump_payload():
    try:
        df = pd.read_csv(conf['catchup_buffer_path'])
        save_dataframe_to_cassandra(session, df, conf['cassandra_table_name'])
        clear_buffer()
    except Exception as e:
        raise e


while True:
    get_payload(date_offset, conf)
    preprocess_payload()
    dump_payload()
