# ipeds_config.py
"""
Configuration settings for the IPEDS data loader.
This file defines file paths and the mapping between the source Access database
and the destination MSSQL database.
"""

# The full, raw string path to your local Access database file.
ACCESS_DB_PATH = r"C:\Users\pgva005\Documents\EPRI_Prj\Meta data\IPEDS202223.accdb"

# Metadata dictionary that defines the ETL (Extract, Transform, Load) process
# for the universities data.
UNIVERSITIES_METADATA = {
    # The name of the table in the Access database to read from.
    "source_table": "HD2022",

    # The name of the stored procedure in MSSQL to execute for the upsert.
    "destination_procedure": "dbo.upsert_universities",

    # Defines the mapping from source columns (in Access) to destination columns (in MSSQL).
    # The order of keys here determines the SELECT statement order.
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
    }
}
