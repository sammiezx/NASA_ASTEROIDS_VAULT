from datetime import datetime, timedelta
import pandas as pd
import os
from dotenv import load_dotenv
import yaml
import time
import logging

from client import hit_nasa_api
from adapter import parse_response
from connector.cassandra_connector import get_session, create_and_set_keyspace, create_table, save_dataframe_to_cassandra, get_min_max_dates

class CatchStream:

    
    def __init__(self):
        load_dotenv()
        with open(os.getenv("CONF_PATH"), 'r') as file:
            conf = yaml.load(file, Loader=yaml.FullLoader)
        
        session = get_session()
        create_and_set_keyspace(session, conf['cassandra_keyspace_name'])
        create_table(session, conf['cassandra_table_name'])

        stream_id = datetime.now().date().strftime('CATCH_STREAM_%Y-%m-%dT%H:%M:%S')

        self.conf = conf
        self.session = session
        self.date_offset, _ = get_min_max_dates(session)

        logging.basicConfig(filename= conf['log_path']+ stream_id + '.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__ + f'.instance_{id(self)}')
        

    def clear_buffer(self):
        file_path = self.conf['catchup_buffer_path']
        try:
            os.remove(file_path)
            print(f"File '{file_path}' has been deleted.")
        except FileNotFoundError:
            print(f"File '{file_path}' not found.")
        except Exception as e:
            print(f"An error occurred: {e}")


    def get_payload(self):
        self.logger.info("[TARGETING NASA API]")
        conf = self.conf

        date_offset = self.date_offset
        if(date_offset < datetime.strptime(conf['catch_upto'], "%Y-%m-%d")):
            raise Exception("CAUGHT UP!!, [you can disable the dag now]")
        
        end_date = date_offset.strftime('%Y-%m-%d')
        start_date = (date_offset - timedelta(days=6)).strftime('%Y-%m-%d')
        self.date_offset = date_offset - timedelta(days=6)
        print(start_date, end_date)

        try:
            df = hit_nasa_api(start_date, end_date)
            df.to_csv(conf['catchup_buffer_path'], index=False)
            self.logger.info(f"[PAYLOAD INITIALIZED FOR]: {start_date} TO {end_date} batch")
            print(f"[PAYLOAD INITIALIZED FOR]: {start_date} TO {end_date} batch")
            return
        except Exception as e:
            raise e

    def preprocess_payload(self):
        self.logger.info("[PROCESSING PAYLOAD]")
        conf = self.conf

        try:
            df = pd.read_csv(conf['catchup_buffer_path'])
            df = parse_response(df)
            # adapter takes care of most of the parsing and all, while more can be added in this task later on
            self.clear_buffer()
            df.to_csv(conf['catchup_buffer_path'], index=False)
            # self.logger.info("[PAYLOAD PROCESSED FOR]: {start_date} TO {end_date} batch")
        except Exception as e:
            raise e

    def dump_payload(self):
        self.logger.info("[DUMPING PAYLOAD]")
        conf = self.conf

        try:
            df = pd.read_csv(conf['catchup_buffer_path'])
            df['neo_reference_id'] = df['neo_reference_id'].astype(str)
            df['miss_distance_astronomical'] = df['miss_distance_astronomical'].astype(str)
            df['miss_distance_lunar'] = df['miss_distance_lunar'].astype(str)
            df['id'] = df['id'].astype(str)

            save_dataframe_to_cassandra(self.session, df, conf['cassandra_table_name'])
            self.clear_buffer()
            # self.logger.info(f"[PAYLOAD DUMPED FOR]: {start_date} TO {end_date} batch")
        except Exception as e:
            raise e

    def stream(self):
        while True:
            try:
                self.get_payload()
                self.preprocess_payload()
                self.dump_payload()
                # time.sleep(100)
            except Exception as e:
                self.logger.error("An error occurred: %s", str(e), exc_info=True)
                raise e
