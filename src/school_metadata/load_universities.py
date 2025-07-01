# load_universities.py
import os
import sys
import pyodbc
import logging
from typing import List, Tuple, Any

# Import settings from our configuration file
from ipeds_config import ACCESS_DB_PATH, UNIVERSITIES_METADATA

# --- Basic Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    stream=sys.stdout
)

def get_access_data() -> List[Tuple[Any, ...]]:
    """
    Connects to the local MS Access database, extracts data from the HD2022 table,
    and returns it as a list of rows.
    """
    logging.info(f"Attempting to connect to Access database at: {ACCESS_DB_PATH}")
    
    # Connection string for the 64-bit Microsoft Access ODBC driver
    access_conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={ACCESS_DB_PATH};'
    )
    
    source_table = UNIVERSITIES_METADATA["source_table"]
    source_columns = ", ".join(UNIVERSITIES_METADATA["column_mapping"].keys())
    query = f"SELECT {source_columns} FROM {source_table};"
    
    rows = []
    try:
        with pyodbc.connect(access_conn_str) as conn:
            logging.info("Access connection successful.")
            cursor = conn.cursor()
            logging.info(f"Executing query: {query}")
            cursor.execute(query)
            rows = cursor.fetchall()
            logging.info(f"Successfully extracted {len(rows)} rows from Access table '{source_table}'.")
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logging.error(
            f"Failed to connect to or read from Access DB. SQLSTATE: {sqlstate}. "
            f"Ensure the path is correct and the 64-bit Microsoft Access Database Engine driver is installed."
        )
        raise
        
    return rows

def transform_data_for_mssql(rows: List[pyodbc.Row]) -> List[Tuple[Any, ...]]:
    """
    Transforms the raw data from Access into the format required by the MSSQL
    User-Defined Table Type (UDTT).
    """
    tvp_rows = []
    for row in rows:
        # Convert the pyodbc.Row object to a list to allow modification
        record = list(row)
        
        # The HBCU column is the 11th column (index 10) based on the mapping order.
        # IPEDS uses 1 for Yes, 2 for No. We need 1 for Yes, 0 for No.
        if len(record) > 10 and record[10] == 2:
            record[10] = 0
            
        tvp_rows.append(tuple(record))
        
    logging.info("Data transformation complete.")
    return tvp_rows

def load_data_to_mssql(data_to_load: List[Tuple[Any, ...]]):
    """
    Connects to the MSSQL server and performs a bulk upsert using a
    stored procedure and a Table-Valued Parameter (TVP).
    """
    # Check for required environment variables
    required_vars = ['DB_SERVER', 'DB_NAME', 'DB_USER', 'DB_PASS']
    if not all(k in os.environ for k in required_vars):
        logging.error(f"Missing one or more environment variables: {required_vars}. Aborting.")
        return

    # Connection string for MSSQL
    mssql_conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_NAME')};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASS')};"
        "TrustServerCertificate=yes;"
        "Encrypt=yes;"
    )
    
    proc_name = UNIVERSITIES_METADATA["destination_procedure"]
    sql_command = f"EXEC {proc_name} ?"
    
    logging.info(f"Attempting to connect to MSSQL server: {os.getenv('DB_SERVER')}")
    try:
        with pyodbc.connect(mssql_conn_str) as conn:
            logging.info("MSSQL connection successful.")
            cursor = conn.cursor()
            
            logging.info(f"Executing bulk upsert via stored procedure '{proc_name}'...")
            
            # --- THIS IS THE FIX ---
            # Pass the list of tuples as the single element inside another list.
            # This tells pyodbc that the entire list of tuples is the one parameter
            # for the stored procedure.
            cursor.execute(sql_command, [data_to_load])
            
            conn.commit()
            logging.info(f"Successfully loaded {len(data_to_load)} records into MSSQL.")
            
    except pyodbc.Error as ex:
        logging.error(f"An error occurred during the MSSQL operation: {ex}")
        raise

if __name__ == "__main__":
    logging.info("--- Starting IPEDS Universities Data Load ---")
    try:
        # Step 1: Extract
        access_rows = get_access_data()
        
        if access_rows:
            # Step 2: Transform
            transformed_rows = transform_data_for_mssql(access_rows)
            
            # Step 3: Load
            load_data_to_mssql(transformed_rows)
        else:
            logging.warning("No data was extracted from Access, so nothing will be loaded.")
            
    except Exception as e:
        logging.critical(f"The script failed with an unhandled exception: {e}")
        
    logging.info("--- Data Load Process Finished ---")
