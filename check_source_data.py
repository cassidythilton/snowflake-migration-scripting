#!/usr/bin/env python3
"""
Source Data Inspector for Snowflake Tables

This utility script inspects source tables before migration to:
- Verify data exists and is accessible
- Sample data for quality assessment
- Check row counts and basic statistics
- Validate source table structure

Configure the connection details below to match your source environment.
"""

import logging
from snowflake.snowpark import Session

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION - UPDATE THESE VALUES
# ──────────────────────────────────────────────────────────────────────────────

# Source connection config (update with your source account details)
SOURCE = {
    "account":       "YOUR_SOURCE_ACCOUNT",        # e.g., "ABC12345-XY67890"
    "user":          "YOUR_USERNAME",              # Your Snowflake username
    "role":          "ACCOUNTADMIN",               # Or role with sufficient privileges
    "warehouse":     "COMPUTE_WH",                 # Warehouse to use for queries
    "authenticator": "SNOWFLAKE_JWT",
    "private_key_file": "/path/to/your/rsa_key.p8",  # Path to your RSA private key
}

# Database and schema to inspect
SOURCE_DB = "YOUR_DATABASE"          # Source database name
SOURCE_SCHEMA = "YOUR_SCHEMA"        # Source schema name

# Tables to inspect (add/remove as needed)
TABLES = [
    "TABLE1",
    "TABLE2", 
    "TABLE3",
    # Add your table names here
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def check_source_table_data(sess: Session, table: str):
    """Check sample data in source table"""
    fq_table = f'"{SOURCE_DB}"."{SOURCE_SCHEMA}"."{table}"'
    
    logging.info(f"\n=== Checking SOURCE {table} ===")
    
    # Sample first 3 rows
    sample_query = f"SELECT * FROM {fq_table} LIMIT 3"
    sample_data = sess.sql(sample_query).collect()
    logging.info(f"Sample data (first 3 rows):")
    for i, row in enumerate(sample_data):
        logging.info(f"  Row {i+1}: {row}")
    
    # Total row count
    total_query = f"SELECT COUNT(*) FROM {fq_table}"
    total_rows = sess.sql(total_query).collect()[0][0]
    logging.info(f"Total rows: {total_rows}")

def main():
    logging.info("Connecting to SOURCE Snowflake...")
    sess = Session.builder.configs(SOURCE).create()
    
    try:
        sess.sql(f'USE DATABASE "{SOURCE_DB}"').collect()
        sess.sql(f'USE SCHEMA "{SOURCE_DB}"."{SOURCE_SCHEMA}"').collect()
        
        for table in TABLES:
            try:
                check_source_table_data(sess, table)
            except Exception as e:
                logging.error(f"Error checking source {table}: {e}")
                
    finally:
        sess.close()

if __name__ == "__main__":
    main()
