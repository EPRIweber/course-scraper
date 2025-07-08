# load_graduation_rates.py
import os
import sys
import pyodbc
import logging
from typing import List, Tuple, Any

# Import settings from our configuration file
from ipeds_config import IPEDS_DATA_REPOSITORY_PATH, GRADUATION_RATES_METADATA

# --- Basic Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] - %(message)s',
    stream=sys.stdout
)

def get_access_data(db_path: str, table_name: str, report_year: int) -> List[Tuple[Any, ...]]:
    """
    Connects to a specific MS Access database, extracts graduation rate data,
    and injects the report_year into each record.
    """
    logging.info(f"Attempting to connect to Access database at: {db_path}")
    
    access_conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={db_path};'
    )
    
    # --- THIS IS THE FIX ---
    # Use a 'SELECT *' query to avoid any potential parsing issues with column names.
    # This sends the simplest possible query to the problematic ODBC driver.
    query = f"SELECT * FROM [{table_name}]"
    
    data_to_load = []
    try:
        with pyodbc.connect(access_conn_str) as conn:
            logging.info("Access connection successful.")
            cursor = conn.cursor()
            logging.info(f"Executing query: {query}")
            cursor.execute(query)
            rows = cursor.fetchall()
            logging.info(f"Successfully extracted {len(rows)} rows from Access table '{table_name}'.")

            # Transform rows into the format required by the UDTT, adding the report year.
            # We can access columns by name from the pyodbc.Row object.
            for row in rows:
                # The tuple must be in the order defined by the UDTT: (report_year, unitid, grtype, grrtot)
                record = (report_year, row.UNITID, row.GRTYPE, row.GRRTOT)
                data_to_load.append(record)
            logging.info("Data transformation complete.")

    except pyodbc.Error as ex:
        # Handle cases where a table might not exist for a given year
        if ex.args[0] == '42S02': # 'Invalid object name' or 'table not found' error
            logging.warning(f"Table '{table_name}' not found in {db_path}. Skipping this year.")
            return []
        logging.error(f"Failed to connect to or read from Access DB. SQLSTATE: {ex.args[0]}.")
        raise
        
    return data_to_load

def load_data_to_mssql(data_to_load: List[Tuple[Any, ...]]):
    """
    Connects to the MSSQL server and performs a bulk upsert using a
    stored procedure and a Table-Valued Parameter (TVP).
    """
    required_vars = ['DB_SERVER', 'DB_NAME', 'DB_USER', 'DB_PASS']
    if not all(k in os.environ for k in required_vars):
        logging.error(f"Missing one or more environment variables: {required_vars}. Aborting.")
        return

    mssql_conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_NAME')};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASS')};"
        "TrustServerCertificate=yes;"
        "Encrypt=yes;"
    )
    
    proc_name = GRADUATION_RATES_METADATA["destination_procedure"]
    sql_command = f"EXEC {proc_name} ?"
    
    logging.info(f"Attempting to connect to MSSQL server: {os.getenv('DB_SERVER')}")
    try:
        with pyodbc.connect(mssql_conn_str) as conn:
            logging.info("MSSQL connection successful.")
            cursor = conn.cursor()
            
            logging.info(f"Executing bulk upsert via stored procedure '{proc_name}'...")
            cursor.execute(sql_command, [data_to_load])
            conn.commit()
            logging.info(f"Successfully loaded {len(data_to_load)} records into MSSQL.")
            
    except pyodbc.Error as ex:
        logging.error(f"An error occurred during the MSSQL operation: {ex}")
        raise

if __name__ == "__main__":
    logging.info("--- Starting IPEDS Graduation Rates Bulk Data Load ---")
    
    cfg = GRADUATION_RATES_METADATA
    start_year = cfg["start_year"]
    end_year = cfg["end_year"]

    for year in range(start_year, end_year + 1):
        # The report year is the end of the academic year (e.g., for 2004-2005, report year is 2005)
        report_year = year + 1
        
        # Construct file and table names based on templates
        short_end_year = str(report_year)[-2:]
        file_name = cfg["file_name_template"].format(year, short_end_year)
        table_name = cfg["table_name_template"].format(year)
        db_path = os.path.join(IPEDS_DATA_REPOSITORY_PATH, file_name)

        logging.info(f"--- Processing Year: {year}-{report_year} ---")

        if not os.path.exists(db_path):
            logging.warning(f"Database file not found: {db_path}. Skipping this year.")
            continue

        try:
            # Step 1: Extract and Transform data for the current year
            transformed_rows = get_access_data(db_path, table_name, report_year)
            
            # Step 2: Load data to MSSQL
            if transformed_rows:
                load_data_to_mssql(transformed_rows)
            else:
                logging.warning(f"No data was extracted for year {year}, so nothing will be loaded.")
                
        except Exception as e:
            logging.critical(f"The script failed while processing year {year}. Error: {e}")
    
    logging.info("--- Bulk Data Load Process Finished ---")
