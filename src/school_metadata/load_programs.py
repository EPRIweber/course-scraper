# load_programs.py
import os
import sys
import csv
import pyodbc
import logging
from typing import List, Tuple

# Import settings from our configuration file
from ipeds_config import PROGRAMS_CSV_PATH, PROGRAMS_METADATA

# --- Basic Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    stream=sys.stdout
)

def get_csv_data() -> List[Tuple]:
    """
    Reads program data from the specified CSV file.
    """
    logging.info(f"Attempting to read CSV file at: {PROGRAMS_CSV_PATH}")
    
    rows = []
    try:
        with open(PROGRAMS_CSV_PATH, mode='r', encoding='utf-8-sig') as infile:
            # utf-8-sig handles the potential BOM (Byte Order Mark) at the start of the file.
            reader = csv.reader(infile)
            
            # Read the header row to map columns by name
            header = next(reader)
            column_map = PROGRAMS_METADATA["column_mapping"]
            
            # Create a list of indices based on the mapping order to ensure correctness
            try:
                indices = [header.index(col_name) for col_name in column_map.keys()]
            except ValueError as e:
                logging.error(f"A required column was not found in the CSV header: {e}")
                return []

            # Process each row in the CSV
            for row in reader:
                # Extract data using the determined indices
                selected_data = tuple(row[i] for i in indices)
                rows.append(selected_data)
                
        logging.info(f"Successfully extracted {len(rows)} rows from CSV file.")
        
    except FileNotFoundError:
        logging.error(f"The file was not found at the specified path: {PROGRAMS_CSV_PATH}")
        raise
    except Exception as e:
        logging.error(f"An error occurred while reading the CSV file: {e}")
        raise
        
    return rows

def load_data_to_mssql(data_to_load: List[Tuple]):
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
    
    proc_name = PROGRAMS_METADATA["destination_procedure"]
    sql_command = f"EXEC {proc_name} ?"
    
    logging.info(f"Attempting to connect to MSSQL server: {os.getenv('DB_SERVER')}")
    try:
        with pyodbc.connect(mssql_conn_str) as conn:
            logging.info("MSSQL connection successful.")
            cursor = conn.cursor()
            
            logging.info(f"Executing bulk upsert via stored procedure '{proc_name}'...")
            
            # Pass the list of tuples as the single element inside another list.
            cursor.execute(sql_command, [data_to_load])
            
            conn.commit()
            logging.info(f"Successfully loaded {len(data_to_load)} records into MSSQL.")
            
    except pyodbc.Error as ex:
        logging.error(f"An error occurred during the MSSQL operation: {ex}")
        raise

if __name__ == "__main__":
    logging.info("--- Starting IPEDS Programs Data Load ---")
    try:
        # Step 1: Extract data from CSV
        csv_rows = get_csv_data()
        
        if csv_rows:
            # Step 2: Load data to MSSQL (no transformation needed for this data)
            load_data_to_mssql(csv_rows)
        else:
            logging.warning("No data was extracted from CSV, so nothing will be loaded.")
            
    except Exception as e:
        logging.critical(f"The script failed with an unhandled exception: {e}")
        
    logging.info("--- Data Load Process Finished ---")
