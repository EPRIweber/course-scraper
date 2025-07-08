# ipeds_config.py
"""
Configuration settings for the IPEDS data loader.
This file defines file paths and the mapping between source files
and the destination MSSQL database.
"""

# --- Main Data Repository Path ---
# The folder where all your yearly IPEDS Access databases are stored.
IPEDS_DATA_REPOSITORY_PATH = r"C:\Users\pgva005\Documents\EPRI_Prj\Meta data"


# --- Universities Loader Configuration (remains the same) ---
# For loading a single year's data as before.
UNIVERSITIES_METADATA = {
    "source_db": "IPEDS202223.accdb", # The specific DB file in the repository
    "source_table": "HD2022",
    "destination_procedure": "dbo.upsert_universities",
    "column_mapping": {
        "UNITID": "unitid",
        "INSTNM": "instnm",
        "ADDR": "addr",
        "CITY": "city",
        "STABBR": "stabbr",
        "ZIP": "zip",
        "WEBADDR": "webaddr",
        "CONTROL": "control",
        "SECTOR": "sector",
        "C18BASIC": "c18basic",
        "HBCU": "hbcu",
        "C21SZSET": "c21szset",
    }
}


# --- Programs Loader Configuration (remains the same) ---
PROGRAMS_CSV_PATH = r"C:\Users\pgva005\Downloads\CIPCode2020.csv"
PROGRAMS_METADATA = {
    "destination_procedure": "dbo.upsert_programs",
    "column_mapping": { "CIPCode": "cipcode", "CIPTitle": "program_name", "CIPDefinition": "program_description" }
}


# --- Graduation Rates Loader Configuration (NOW DYNAMIC) ---
GRADUATION_RATES_METADATA = {
    # Define the range of years you want to process.
    # The script will loop from start_year to end_year inclusive.
    "start_year": 2004,
    "end_year": 2022,

    # Define the naming patterns for your files and tables.
    # The script will replace {} with the year from the loop.
    # e.g., IPEDS200405.accdb, GR2004
    "file_name_template": "IPEDS{}{}.accdb",
    "table_name_template": "GR{}",

    # Destination stored procedure in MSSQL.
    "destination_procedure": "dbo.upsert_graduation_rates",

    # Column mapping from the source table.
    "column_mapping": { "UNITID": "unitid", "GRTYPE": "grtype", "GRRTOT": "grrtot" }
}
