
import pandas as pd
import requests
from io import BytesIO
import sqlite3
import os
import io
import zipfile
import urllib3
from urllib3.exceptions import InsecureRequestWarning

urllib3.disable_warnings(InsecureRequestWarning)
    

def fetch_csv(url):
    response = requests.get(url)
    res = response.content.decode('utf-8')
    df = pd.read_csv(io.StringIO(res))
    return df

def save_sqlite(df, table_name, db_path):
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def download_zip(url, headers=None):
    response = requests.get(url, headers=headers, verify=False)
    response.raise_for_status()
    with zipfile.ZipFile(BytesIO(response.content)) as zfile:
        for file_name in zfile.namelist():
            if file_name.endswith('.csv') and 'Population by Age and Sex - US, States, Counties' in file_name:
                with zfile.open(file_name) as extracted_file:
                    df =  pd.read_csv(extracted_file, encoding='latin1')
    return df

url = 'https://www.statsamerica.org/downloads/Population-by-Age-and-Sex.zip'
url1 = 'https://data.cityofnewyork.us/api/views/btm5-ppia/rows.csv'
df1 = fetch_csv(url1)
df = download_zip(url)
new_york_data = df[df['Description'] == 'New York']
new_york_data_filtered = new_york_data.drop(columns=['IBRC_Geo_ID', 'Statefips', 'Countyfips'])

traffic_data_filtered = df1.drop(columns=['ID', 'SegmentID', 'From', 'To', 'Direction'])
traffic_data_filtered_cleaned = traffic_data_filtered.dropna()


db_path = os.path.join('../data', 'data_base.db')
sqlite_db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'data_base.db'))
save_sqlite(new_york_data_filtered, 'New York Population', sqlite_db_path)
save_sqlite(traffic_data_filtered_cleaned, 'New York Traffic',sqlite_db_path)