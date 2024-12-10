import sqlite3
import os

# Path to the SQLite database
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
DATABASE_PATH = os.path.join(BASE_DIR, '../data/data_base.db')


def test_database_exists():
    assert os.path.exists(DATABASE_PATH), "Database file does not exist."

def test_tables_exist():
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    # List tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    connection.close()

    expected_tables = {'New_York_Population', 'New_York_Traffic'}
    existing_tables = {table[0] for table in tables}

    assert expected_tables.issubset(existing_tables), f"Missing tables: {expected_tables - existing_tables}"

def test_table_non_empty():
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    # Validate that tables are non-empty
    cursor.execute("SELECT COUNT(*) FROM New_York_Population;")
    assert cursor.fetchone()[0] > 0, "New_York_Population table is empty."

    cursor.execute("SELECT COUNT(*) FROM New_York_Traffic;")
    assert cursor.fetchone()[0] > 0, "New_York_Traffic table is empty."

    connection.close()

def test_table_columns():
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    expected_columns_population = [
        ("Year", "INTEGER"),
        ("Total Population", "REAL"),
        ("Male Population", "REAL"),
        ("Female Population", "REAL"),
    ]
    expected_columns_traffic = [
        ("roadway_name", "TEXT"),
        ("date", "TEXT"),
        ("1200_400_am", "REAL"),
        ("400_800_am", "REAL"),
        ("800_1200_am", "REAL"),
        ("1200_400_pm", "REAL"),
        ("400_800_pm", "REAL"),
        ("800_1200_pm", "REAL"),
    ]

    # Check columns for New_York_Population
    cursor.execute("PRAGMA table_info(New_York_Population);")
    population_columns = [(col[1], col[2]) for col in cursor.fetchall()]
    for column, data_type in expected_columns_population:
        assert (column, data_type) in population_columns, f"Column '{column}' with type '{data_type}' not found in 'New_York_Population'"

    # Check columns for New_York_Traffic
    cursor.execute("PRAGMA table_info(New_York_Traffic);")
    traffic_columns = [(col[1], col[2]) for col in cursor.fetchall()]
    for column, data_type in expected_columns_traffic:
        assert (column, data_type) in traffic_columns, f"Column '{column}' with type '{data_type}' not found in 'New_York_Traffic'"

    connection.close()


def test_data_validity():
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    #Check ranges for New_York_Population
    cursor.execute("SELECT MAX([Total Population]), MIN([Total Population]) FROM New_York_Population;")
    max_value, min_value = cursor.fetchone()
    assert min_value >= 0, "Population cannot be negative."
    assert max_value <= 40000000, "Population exceeds realistic range."

    # List of traffic interval columns
    traffic_columns = [
        "1200_400_am",
        "400_800_am",
        "800_1200_am",
        "1200_400_pm",
        "400_800_pm",
        "800_1200_pm",
    ]

    # Validate ranges for all traffic interval columns
    for column in traffic_columns:
        cursor.execute(f"SELECT MAX([{column}]), MIN([{column}]) FROM New_York_Traffic;")
        max_value, min_value = cursor.fetchone()
        assert min_value >= 0, f"Traffic volume in column '{column}' cannot be negative."
        assert max_value <= 100000, f"Traffic volume in column '{column}' exceeds realistic range."

    connection.close()

def test_population_year_range():
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    cursor.execute("SELECT MIN(Year), MAX(Year) FROM New_York_Population;")
    min_year, max_year = cursor.fetchone()
    assert min_year >= 1900, "Year in New_York_Population is before 1900."
    assert max_year <= 2100, "Year in New_York_Population is beyond 2100."

    connection.close()


def test_no_null_values():
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    # Check New_York_Population table
    cursor.execute("SELECT COUNT(*) FROM New_York_Population WHERE [Year] IS NULL OR [Total Population] IS NULL;")
    assert cursor.fetchone()[0] == 0, "NULL values found in critical columns of New_York_Population."

    # Check New_York_Traffic table
    cursor.execute("SELECT COUNT(*) FROM New_York_Traffic WHERE [roadway_name] IS NULL OR [date] IS NULL;")
    assert cursor.fetchone()[0] == 0, "NULL values found in critical columns of New_York_Traffic."

    connection.close()

if __name__ == "__main__":
    test_database_exists()
    test_tables_exist()
    test_table_non_empty()
    test_table_columns()
    test_data_validity()
    test_population_year_range()
    test_no_null_values()
