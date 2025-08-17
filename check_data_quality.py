#!/usr/bin/env python3
"""
Data Quality Checker for Snowflake Tables

This utility script checks for data quality issues in migrated tables including:
- NULL value analysis
- Sample data inspection
- Row count verification
- Column structure validation

Configure the connection details below to match your target environment.
"""

import logging
from snowflake.snowpark import Session

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION - UPDATE THESE VALUES
# ──────────────────────────────────────────────────────────────────────────────

# Target connection config (update with your target account details)
TARGET = {
    "account":       "YOUR_TARGET_ACCOUNT",        # e.g., "ABC12345-XY67890"
    "user":          "YOUR_USERNAME",              # Your Snowflake username
    "role":          "ACCOUNTADMIN",               # Or role with sufficient privileges
    "warehouse":     "COMPUTE_WH",                 # Warehouse to use for queries
    "authenticator": "SNOWFLAKE_JWT",
    "private_key_file": "/path/to/your/rsa_key.p8",  # Path to your RSA private key
}

# Database and schema to check
TARGET_DB = "YOUR_DATABASE"          # Target database name
TARGET_SCHEMA = "YOUR_SCHEMA"        # Target schema name

# Tables to analyze (add/remove as needed)
TABLES = [
    "TABLE1",
    "TABLE2", 
    "TABLE3",
    # Add your table names here
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def check_table_data(sess: Session, table: str):
    """Check sample data and null counts for a table"""
    fq_table = f'"{TARGET_DB}"."{TARGET_SCHEMA}"."{table}"'
    
    logging.info(f"\n=== Checking {table} ===")
    
    # Get column names
    columns_query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM "{TARGET_DB}".INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{TARGET_SCHEMA}' AND TABLE_NAME = '{table}'
    ORDER BY ORDINAL_POSITION
    """
    columns = sess.sql(columns_query).collect()
    logging.info(f"Columns: {[(c[0], c[1]) for c in columns]}")
    
    # Sample first 5 rows
    sample_query = f"SELECT * FROM {fq_table} LIMIT 5"
    sample_data = sess.sql(sample_query).collect()
    logging.info(f"Sample data (first 5 rows):")
    for i, row in enumerate(sample_data):
        logging.info(f"  Row {i+1}: {row}")
    
    # Check for NULL values in each column
    null_counts = {}
    for col_name, data_type in columns:
        null_query = f"SELECT COUNT(*) as null_count FROM {fq_table} WHERE \"{col_name}\" IS NULL"
        null_count = sess.sql(null_query).collect()[0][0]
        if null_count > 0:
            null_counts[col_name] = null_count
    
    if null_counts:
        logging.warning(f"NULL value counts for {table}: {null_counts}")
    else:
        logging.info(f"✅ No NULL values found in {table}")
    
    # Total row count
    total_query = f"SELECT COUNT(*) FROM {fq_table}"
    total_rows = sess.sql(total_query).collect()[0][0]
    logging.info(f"Total rows: {total_rows}")
    
    return null_counts

def main():
    logging.info("Connecting to target Snowflake...")
    sess = Session.builder.configs(TARGET).create()
    
    try:
        sess.sql(f'USE DATABASE "{TARGET_DB}"').collect()
        sess.sql(f'USE SCHEMA "{TARGET_DB}"."{TARGET_SCHEMA}"').collect()
        
        all_null_issues = {}
        
        for table in TABLES:
            try:
                null_counts = check_table_data(sess, table)
                if null_counts:
                    all_null_issues[table] = null_counts
            except Exception as e:
                logging.error(f"Error checking {table}: {e}")
        
        logging.info("\n" + "="*60)
        logging.info("DATA QUALITY SUMMARY")
        logging.info("="*60)
        
        if all_null_issues:
            logging.warning("Tables with NULL values found:")
            for table, null_counts in all_null_issues.items():
                logging.warning(f"  {table}: {null_counts}")
        else:
            logging.info("✅ No NULL value issues detected in any table")
            
    finally:
        sess.close()

if __name__ == "__main__":
    main()
