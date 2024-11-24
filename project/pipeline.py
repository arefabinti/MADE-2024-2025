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

# Fetch CSV data from a URL
def fetch_csv(url):
    response = requests.get(url)
    content = response.content.decode('utf-8')
    csv_data = pd.read_csv(io.StringIO(content))
    return csv_data

# Save a DataFrame to SQLite database
def save_to_sqlite(dataframe, table_name, database_path):
    connection = sqlite3.connect(database_path)
    dataframe.to_sql(table_name, connection, if_exists='replace', index=False)
    connection.close()

# Download and extract CSV from a ZIP file
def download_and_extract_zip(url, headers=None):
    response = requests.get(url, headers=headers, verify=False)
    response.raise_for_status()
    with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
        for file_name in zip_file.namelist():
            if file_name.endswith('.csv') and 'Population by Age and Sex - US, States, Counties' in file_name:
                with zip_file.open(file_name) as extracted_file:
                    csv_data = pd.read_csv(extracted_file, encoding='latin1')
    return csv_data

# Filter New York population data
def filter_population_for_new_york(dataframe):
    ny_population_data = dataframe[dataframe['Description'] == 'New York']
    filtered_population_data = ny_population_data.drop(columns=['IBRC_Geo_ID', 'Statefips', 'Countyfips'])
    return filtered_population_data

# Process and aggregate traffic data for New York
def filter_and_aggregate_traffic_data(dataframe):
    # Drop unnecessary columns
    traffic_data = dataframe.drop(columns=['ID', 'SegmentID', 'From', 'To', 'Direction'], errors='ignore')
    traffic_data_cleaned = traffic_data.dropna().copy()

    # Standardize column names
    traffic_data_cleaned.columns = [
        col.strip().replace(" ", "_").replace("-", "_").replace(":", "").replace("/", "").lower() for col in traffic_data_cleaned.columns
    ]

    # Define interval mapping for aggregation
    interval_mapping = {
        '1200_100_am': '1200_400_am',
        '100_200am': '1200_400_am',
        '200_300am': '1200_400_am',
        '300_400am': '1200_400_am',
        '400_500am': '400_800_am',
        '500_600am': '400_800_am',
        '600_700am': '400_800_am',
        '700_800am': '400_800_am',
        '800_900am': '800_1200_pm',
        '900_1000am': '800_1200_pm',
        '1000_1100am': '800_1200_pm',
        '1100_1200pm': '800_1200_pm',
        '1200_100pm': '1200_400_pm',
        '100_200pm': '1200_400_pm',
        '200_300pm': '1200_400_pm',
        '300_400pm': '1200_400_pm',
        '400_500pm': '400_800_pm',
        '500_600pm': '400_800_pm',
        '600_700pm': '400_800_pm',
        '700_800pm': '400_800_pm',
        '800_900pm': '800_1200_am',
        '900_1000pm': '800_1200_am',
        '1000_1100pm': '800_1200_am',
        '1100_1200am': '800_1200_am',
    }

    # Fill missing values with zero
    traffic_data_cleaned.fillna(0, inplace=True)

    # Create a new DataFrame for aggregated data
    aggregated_traffic_data = traffic_data_cleaned[['roadway_name', 'date']].copy()
    for new_interval in set(interval_mapping.values()):
        columns_to_aggregate = [col for col, interval in interval_mapping.items() if interval == new_interval]
        #print(f"Aggregating columns for {new_interval}: {columns_to_aggregate}")  # Debugging output
        aggregated_traffic_data[new_interval] = traffic_data_cleaned[columns_to_aggregate].sum(axis=1)

    return aggregated_traffic_data

def main():
    # URLs for data
    population_data_url = 'https://www.statsamerica.org/downloads/Population-by-Age-and-Sex.zip'
    traffic_data_url = 'https://data.cityofnewyork.us/api/views/btm5-ppia/rows.csv'

    # Fetch and process data
    traffic_data = fetch_csv(traffic_data_url)
    population_data = download_and_extract_zip(population_data_url)

    # Filter and process data for New York
    filtered_population_data = filter_population_for_new_york(population_data)
    aggregated_traffic_data = filter_and_aggregate_traffic_data(traffic_data)

    # Save data to SQLite
    sqlite_database_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'data_base.db'))
    save_to_sqlite(filtered_population_data, 'New_York_Population', sqlite_database_path)
    save_to_sqlite(aggregated_traffic_data, 'New_York_Traffic', sqlite_database_path)


if __name__ == "__main__":
    main()
    