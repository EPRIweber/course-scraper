# src/ipeds_metadata.py
"""
This file contains the metadata configuration for loading IPEDS data.
It defines the source-to-destination mapping for various tables.
"""

# Mapping for the HD2022 Access table to the dbo.universities MSSQL table.
# The key is the source column name from the Access table (HD2022).
# The value is the destination column name in the MSSQL table (universities).
UNIVERSITIES_METADATA = {
    "source_table": "HD2022",
    "destination_table": "universities",
    "stored_procedure": "dbo.upsert_universities",
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