import os
import requests
import pandas as pd
import sqlite3

# Define the URLs of the datasets (replace with real URLs if necessary)
url_employment_data_csv = "https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.CD?downloadformat=csv"  
url_economic_data_csv = "https://rplumber.ilo.org/data/indicator/?id=UNE_2UNE_SEX_AGE_GEO_NB_A&type=label&format=.csv" 

# Define the directories for storing data
data_directory = r'data'  # Your updated directory path
if not os.path.exists(data_directory):
    os.makedirs(data_directory)

# Function to download a direct CSV file
def download_csv(url, destination_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(destination_path, 'wb') as file:
            file.write(response.content)
        print(f"CSV file downloaded and saved to {destination_path}")
    else:
        print(f"Failed to download CSV from {url}. Status code: {response.status_code}")

# Function to read and clean the CSV file with additional encoding checks
def process_csv(file_path, skip_rows=0):
    encodings_to_try = ['utf-8', 'latin1', 'ISO-8859-1', 'cp1252', 'utf-16']
    for encoding in encodings_to_try:
        try:
            # Attempt to read the CSV with various encodings
            df = pd.read_csv(file_path, skiprows=skip_rows, encoding=encoding, 
                              on_bad_lines='skip', engine='python')
            print(f"Data from {file_path} loaded successfully with encoding {encoding}.")
            return df
        except UnicodeDecodeError:
            print(f"Failed to decode {file_path} using {encoding} encoding.")
        except pd.errors.ParserError as e:
            print(f"Error parsing {file_path} with {encoding} encoding: {e}")
        except Exception as e:
            print(f"Unexpected error with encoding {encoding}: {e}")
    return None

# Function to export DataFrame to SQLite database
def export_to_sqlite(df, table_name, db_name):
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.commit()
        print(f"Data saved to SQLite table '{table_name}' in {db_name}.")
    except Exception as e:
        print(f"Error exporting to SQLite: {e}")
    finally:
        conn.close()

# Check the current working directory
print("Current working directory:", os.getcwd())

# Download and process the employment data (CSV file)
employment_data_file = os.path.join(data_directory, "employment_data.csv")
download_csv(url_employment_data_csv, employment_data_file)
df_employment = process_csv(employment_data_file)

# Download and process the economic data (CSV file)
economic_data_file = os.path.join(data_directory, "economic_data.csv")
download_csv(url_economic_data_csv, economic_data_file)
df_economic = process_csv(economic_data_file)

# Define the SQLite database file path
sqlite_db_file = os.path.join(data_directory, "covid_employment_impact.db")

# Check if the SQLite file exists after export
print(f"Database file path: {os.path.abspath(sqlite_db_file)}")

# Export DataFrames to SQLite if they are not None
if df_employment is not None:
    export_to_sqlite(df_employment, 'employment_data', sqlite_db_file)
else:
    print("Employment data is empty or missing, not exporting.")

if df_economic is not None:
    export_to_sqlite(df_economic, 'economic_data', sqlite_db_file)

# Verify if the database file exists
if os.path.exists(sqlite_db_file):
    print(f"Database file {sqlite_db_file} successfully created.")
else:
    print(f"Database file {sqlite_db_file} not found.")
