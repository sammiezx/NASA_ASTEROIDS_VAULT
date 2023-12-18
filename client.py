import requests
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

def api_adapter(data, start_date, end_date):
    try:
        df = []
        data = data.json()['near_earth_objects']
        current_date = start_date
        while current_date <= end_date:
            key = current_date.strftime('%Y-%m-%d')
            dff = pd.DataFrame(data[key])
            dff['date_system_recorded'] = pd.to_datetime(key)
            df.append(dff)
            current_date += timedelta(days=1)
        return pd.concat(df, ignore_index=True)
    except:
        raise Exception("[ERROR PARSING RESPONSE]")

    

def hit_nasa_api(start_date='2015-09-07', end_date='2015-09-09'):

    load_dotenv()
    api_key = os.getenv("API_KEY_NASA")
    api_url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={api_key}"

    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    assert (end_date - start_date).days < 7, "Period should be less than 7 days"

    response = requests.get(api_url)
    if response.status_code == 200:
        return api_adapter(data=response, start_date=start_date, end_date=end_date)
    else:
        raise Exception(f"[bad GET request]: {response.json()}")
    