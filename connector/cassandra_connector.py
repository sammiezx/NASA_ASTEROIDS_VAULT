from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time
import subprocess

ref_schema = ['links', 'id', 'neo_reference_id', 'name', 'nasa_jpl_url',
       'absolute_magnitude_h', 'is_potentially_hazardous_asteroid',
       'is_sentry_object', 'date_system_recorded', 'estimated_diameter_max', 'estimated_diameter_min',
       'close_approach_date', 'close_approach_date_full',
       'epoch_date_close_approach', 'relative_velocity',
       'miss_distance_astronomical', 'miss_distance_lunar',
       'miss_distance_meters', 'orbiting_body']

def start_cassandra_cluster():
    subprocess.run(['cassandra', '-f'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
    time.sleep(10)
    subprocess.run(['cassandra', '-f', '-Dcassandra.listen_address=localhost', '-Dcassandra.rpc_address=localhost'], 
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
    time.sleep(10)

def get_session():
    cluster = Cluster(['localhost'])
    return cluster.connect()

def create_and_set_keyspace(session, keyspace_name='asteroid_vault'):
    session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name}"+" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
    session.set_keyspace(keyspace_name)
    return

def create_table(session, table_name='proximity_table'):
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            links TEXT,
            id TEXT PRIMARY KEY,
            neo_reference_id TEXT,
            name TEXT,
            nasa_jpl_url TEXT,
            absolute_magnitude_h DOUBLE,
            is_potentially_hazardous_asteroid BOOLEAN,
            is_sentry_object BOOLEAN,
            date_system_recorded TIMESTAMP,
            estimated_diameter_max DOUBLE,
            estimated_diameter_min DOUBLE,
            close_approach_date TEXT,
            close_approach_date_full TEXT,
            epoch_date_close_approach BIGINT,
            relative_velocity DOUBLE,
            miss_distance_astronomical TEXT,
            miss_distance_lunar TEXT,
            miss_distance_meters DOUBLE,
            orbiting_body TEXT
        );
        """)



def save_dataframe_to_cassandra(session, dataframe, table_name='proximity_table'):
    dataframe = dataframe[ref_schema]
    #above statement reinstates order which can be error prone depending on the architecture
    dataframe['date_system_recorded'] = dataframe['date_system_recorded'].map(lambda x: str(x))

    data_records = dataframe.to_dict(orient='records')
    columns = list(data_records[0].keys())
    values_template = ', '.join(['%s' for _ in columns])

    for record in data_records:
        values = [record[column] for column in columns]

        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({values_template})
        """
        session.execute(insert_query, values)

def get_min_max_dates(session, keyspace='asteroid_vault', table='proximity_table', timestamp_column='date_system_recorded'):
    query = f"SELECT MIN({timestamp_column}), MAX({timestamp_column}) FROM {keyspace}.{table};"
    result = session.execute(query)
    min_date, max_date = result[0]
    return min_date, max_date

def shutdown_cluster(session):
    session.shutdown()