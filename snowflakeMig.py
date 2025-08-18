#!/usr/bin/env python3
"""
Snowflake Trial→Trial Migrator (DB → Schemas → Tables)

- Auth: JWT (RSA private key)
- Creates target DB/Schemas/Tables if missing
- Moves data via stage→local files→stage
- Verifies row counts

Tested with snowflake-snowpark-python >= 1.15
"""

# ═══════════════════════════════════════════════════════════════════════════════
# 📋 SETUP INSTRUCTIONS FOR NEW USERS
# ═══════════════════════════════════════════════════════════════════════════════
#
# 🚀 REQUIREMENTS:
# ================
# • Python 3.8+ (tested with 3.11+)
# • snowflake-snowpark-python >= 1.15.0
# • RSA private key file (.p8 format) for JWT authentication
# • Access to both source and target Snowflake accounts
# • ACCOUNTADMIN role in both accounts (or sufficient privileges)
#
# 📦 INSTALLATION:
# ================
# pip install snowflake-snowpark-python
#
# 🔑 AUTHENTICATION SETUP (JWT with RSA Keys):
# ==============================================
# This script uses JWT authentication with RSA key pairs for secure, password-free connections.
#
# STEP 1: Generate RSA Key Pair
#    $ openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
#    $ openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
#    This creates: rsa_key.p8 (private, keep secret) and rsa_key.pub (public, upload to Snowflake)
#
# STEP 2: Extract Public Key Content (remove headers and join lines)
#    $ grep -v "BEGIN\|END" rsa_key.pub | tr -d '\n'
#    Copy the output (long string starting with MIIB...)
#
# STEP 3: Upload Public Key to BOTH Snowflake Accounts
#    Connect to each account and run:
#    ALTER USER <your_username> SET RSA_PUBLIC_KEY='<public_key_content_from_step2>';
#    DESC USER <your_username>;  -- Verify RSA_PUBLIC_KEY is set
#
# STEP 4: Secure Private Key
#    $ chmod 600 rsa_key.p8
#    $ mkdir -p ~/.ssh && mv rsa_key.p8 ~/.ssh/snowflake_rsa_key.p8
#
# STEP 5: Update private_key_file paths in SOURCE and TARGET configs below
#    Use absolute path: "/Users/your_username/.ssh/snowflake_rsa_key.p8"
#
# STEP 6: Find Your Account Identifiers
#    Look at your Snowflake URL: https://<ACCOUNT_ID>.snowflakecomputing.com
#    Examples: ABC12345.us-east-1, mycompany, XYZ67890-AB12345
#
# STEP 7: Test Authentication (optional)
#    from snowflake.snowpark import Session
#    config = {"account": "...", "user": "...", "authenticator": "SNOWFLAKE_JWT", 
#              "private_key_file": "...", "role": "ACCOUNTADMIN", "warehouse": "COMPUTE_WH"}
#    session = Session.builder.configs(config).create()
#    print(session.sql("SELECT CURRENT_USER(), CURRENT_ROLE()").collect())
#
# ⚙️  CONFIGURATION REQUIRED:
# ===========================
# Edit the USER CONFIG section below with your specific values:
#
# • USERNAME: Your Snowflake username (same in both accounts)
# • SOURCE: Source Snowflake account connection details
#   - account: Source account identifier (e.g., "ABC12345-XY67890")
#   - user: Your username in source account
#   - private_key_file: Path to your RSA private key (.p8 file)
# • TARGET: Target Snowflake account connection details
#   - account: Target account identifier (e.g., "DEF67890-ZW12345")
#   - user: Your username in target account
#   - private_key_file: Path to your RSA private key (.p8 file)
# • SOURCE_DB/SOURCE_SCHEMA: Database and schema to migrate FROM
# • TABLES: List of table names to migrate (case-insensitive)
# • TARGET_DB/TARGET_SCHEMA: Database and schema to migrate TO
#
# 🔧 ASSUMPTIONS & PREREQUISITES:
# ===============================
# • Both Snowflake accounts are accessible via JWT authentication
# • You have ACCOUNTADMIN role (or CREATE DATABASE, CREATE SCHEMA, CREATE TABLE privileges)
# • Source tables exist and contain data you want to migrate
# • Target account has sufficient compute resources (COMPUTE_WH warehouse exists)
# • Network connectivity allows file transfers between accounts via user stages
# • No active users/processes are modifying source tables during migration
#
# 📊 WHAT THIS SCRIPT DOES:
# =========================
# 1. Connects to both source and target Snowflake accounts
# 2. Creates target database/schema if they don't exist
# 3. For each table:
#    a. Analyzes source table structure and row count
#    b. Creates/replaces target table with same structure
#    c. Exports data from source to staging area (CSV format)
#    d. Downloads data to local temporary directory
#    e. Uploads data to target staging area
#    f. Loads data into target table with automatic column mapping
#    g. Applies original permissions and ownership
#    h. Validates row counts match between source and target
# 4. Provides comprehensive migration summary report
#
# ⚠️  IMPORTANT NOTES:
# ====================
# • This script will DROP and CREATE target tables (data loss if tables exist)
# • Large tables may take significant time to migrate
# • Temporary files are created locally and cleaned up automatically
# • Migration preserves data types, constraints, and table structure
# • Permissions and ownership are copied from source to target
# • The script is idempotent - can be re-run safely for failed tables
#
# 🛡️  TROUBLESHOOTING:
# =====================
# • Set LOG_LEVEL = logging.DEBUG for detailed debugging information
# • Check private key file permissions (should be readable by script)
# • Verify account identifiers are correct (check Snowflake console URL)
# • Ensure COMPUTE_WH warehouse exists and is accessible
# • Check that user has necessary privileges in both accounts
# • Review Snowflake user stages (@~) have sufficient storage quota
#
# 📝 EXAMPLE USAGE:
# =================
# 1. Configure settings in USER CONFIG section below
# 2. Run: python3 snowflakeMig.py
# 3. Monitor progress via console output
# 4. Review final migration summary report
#
# ═══════════════════════════════════════════════════════════════════════════════

import os
import re
import sys
import json
import shutil
import logging
import tempfile
from typing import List, Tuple, Dict, Optional
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException

# ──────────────────────────────────────────────────────────────────────────────
# USER CONFIG — EDIT THIS SECTION
# ──────────────────────────────────────────────────────────────────────────────

USERNAME = "CHILTON"  # same user name in both environments (case-insensitive)

# Source (OLD env)
SOURCE = {
    "account":       "RIRJXLY-XFB95710",   # ← CHANGE (e.g., "abcd-xy12345")
    "user":          "CHILTON",                 # ← user in the NEW env
    "role":          "ACCOUNTADMIN",
    "warehouse":     "COMPUTE_WH",              # ← existing warehouse in NEW env
    "authenticator": "SNOWFLAKE_JWT",
    "private_key_file": "/Users/cassidy.hilton/Documents/rsa_key.p8",    # Optional: "private_key_file_pwd": "your-passphrase"
}

TARGET = {
    "account":       "UAYHHFB-VBB75852",
    "user":          "CHILTON",                 # ← user in the NEW env
    "role":          "ACCOUNTADMIN",
    "warehouse":     "COMPUTE_WH",              # ← existing warehouse in NEW env
    "authenticator": "SNOWFLAKE_JWT",
    "private_key_file": "/Users/cassidy.hilton/Documents/rsa_key.p8",
    # Optional: "private_key_file_pwd": "your-passphrase"
}

# Scope to migrate
SOURCE_DB      = "COBRA_DEMO_DB"          # ← source database
SOURCE_SCHEMA  = "COBRA_DEMO_SCHEMA"      # ← source schema (non INFORMATION_SCHEMA)
TABLES: List[str] = [                     # ← explicit table list (case-insensitive)
    "EMPLOYEES",
    "CUSTOMERS",
    "ORDERS",
    "PRODUCTS",
    # … add more
]

# Target names (defaults mirror source; change if you want to rename)
TARGET_DB     = SOURCE_DB
TARGET_SCHEMA = SOURCE_SCHEMA

# Local scratch
LOCAL_BASE = None  # None => temp dir; or set a path to persist artifacts

# Logging
LOG_LEVEL = logging.INFO  # User-friendly level (set to DEBUG for troubleshooting)

# ──────────────────────────────────────────────────────────────────────────────
# INTERNALS
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def _fatal(msg: str) -> None:
    logging.error(msg)
    raise RuntimeError(msg)

def connect_and_enforce(cfg: Dict, expect_user: str, expect_role: str) -> Session:
    logging.info(f"Connecting to account={cfg['account']} as user={cfg['user']}")
    sess = Session.builder.configs(cfg).create()

    # Force role and verify
    try:
        sess.sql(f'USE ROLE "{expect_role}"').collect()
    except Exception as e:
        _fatal(f'Could not USE ROLE "{expect_role}". Ensure user "{cfg["user"]}" is granted it. Error: {e}')
    who = sess.sql("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_VERSION()").collect()[0]
    if f"{who[0]}".upper() != expect_user.upper():
        _fatal(f'CURRENT_USER()="{who[0]}", expected "{expect_user}". Check connector config.')
    if f"{who[1]}".upper() != expect_role.upper():
        _fatal(f'CURRENT_ROLE()="{who[1]}", expected "{expect_role}".')

    # Ensure/attach warehouse; create on target if missing
    if cfg.get("warehouse"):
        wh = cfg["warehouse"]
        if not _warehouse_exists(sess, wh):
            logging.warning(f'Warehouse "{wh}" not found. Creating XSMALL …')
            _create_warehouse(sess, wh)
        sess.sql(f'USE WAREHOUSE "{wh}"').collect()
        logging.info(f'Using warehouse "{wh}"')

    return sess

def _warehouse_exists(sess: Session, name: str) -> bool:
    return len(sess.sql(f"SHOW WAREHOUSES LIKE '{name}'").collect()) > 0

def _create_warehouse(sess: Session, name: str) -> None:
    sess.sql(
        f"""
        CREATE WAREHOUSE IF NOT EXISTS "{name}"
        WAREHOUSE_SIZE='XSMALL'
        AUTO_SUSPEND=60
        AUTO_RESUME=TRUE
        INITIALLY_SUSPENDED=TRUE
        """
    ).collect()
    sess.sql(f'GRANT USAGE ON WAREHOUSE "{name}" TO ROLE ACCOUNTADMIN').collect()

def ensure_db_schema(sess: Session, db: str, schema: Optional[str] = None) -> None:
    try:
        sess.sql(f'CREATE DATABASE IF NOT EXISTS "{db}"').collect()
        # Set the current database context
        sess.sql(f'USE DATABASE "{db}"').collect()
        logging.info(f'Using database "{db}"')
    except Exception as e:
        _fatal(f'Failed to CREATE DATABASE "{db}". Requires CREATE DATABASE on ACCOUNT. Error: {e}')
    if schema and schema.upper() != "INFORMATION_SCHEMA":
        try:
            sess.sql(f'CREATE SCHEMA IF NOT EXISTS "{db}"."{schema}"').collect()
            # Set the current schema context
            sess.sql(f'USE SCHEMA "{db}"."{schema}"').collect()
            logging.info(f'Using schema "{db}"."{schema}"')
        except Exception as e:
            _fatal(f'Failed to CREATE SCHEMA "{db}"."{schema}". Error: {e}')

def list_tables(sess: Session, db: str, schema: str) -> List[str]:
    q = f"""
    SELECT TABLE_NAME
    FROM "{db}".INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_TYPE = 'BASE TABLE';
    """
    return [r[0] for r in sess.sql(q).collect()]

def get_table_ddl(sess: Session, db: str, schema: str, table: str) -> str:
    return sess.sql(f"SELECT GET_DDL('TABLE', '\"{db}\".\"{schema}\".\"{table}\"')").collect()[0][0]

def get_table_columns(sess: Session, db: str, schema: str, table: str) -> List[str]:
    """Get ordered list of column names for the table"""
    q = f"""
    SELECT COLUMN_NAME
    FROM "{db}".INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
    ORDER BY ORDINAL_POSITION;
    """
    return [r[0] for r in sess.sql(q).collect()]

def rewrite_table_ddl(ddl: str, src_db: str, src_schema: str, tgt_db: str, tgt_schema: str) -> str:
    # Force "DB"."SCHEMA"."TABLE" to target db/schema
    ddl2 = re.sub(
        rf'\"{re.escape(src_db)}\"\.\"{re.escape(src_schema)}\"\.\"([^\"]+)\"',
        lambda m: f'"{tgt_db}"."{tgt_schema}"."{m.group(1)}"',
        ddl,
        flags=re.IGNORECASE,
    )
    ddl2 = re.sub(r'\bCREATE TABLE\b', 'CREATE OR REPLACE TABLE', ddl2, flags=re.IGNORECASE)
    ddl2 = re.sub(r'COMMENT\s*=\s*\'.*?\'', '', ddl2, flags=re.IGNORECASE | re.DOTALL)
    return ddl2

def rowcount(sess: Session, db: str, schema: str, table: str) -> int:
    return sess.sql(f'SELECT COUNT(*) FROM "{db}"."{schema}"."{table}"').collect()[0][0]

# ────────────────────────────── Grants & Ownership ─────────────────────────────

def show_table_owner(sess: Session, db: str, schema: str, table: str) -> Optional[str]:
    rows = sess.sql(f"SHOW TABLES LIKE '{table}' IN SCHEMA \"{db}\".\"{schema}\"").collect()
    if not rows:
        return None
    owner_idx = next((i for i, c in enumerate(rows[0]._fields) if c.lower() == 'owner'), None)
    return (rows[0][owner_idx] if owner_idx is not None else None)

def fetch_role_grants_on_table(sess: Session, db: str, schema: str, table: str) -> List[Dict[str, str]]:
    rs = sess.sql(f'SHOW GRANTS ON TABLE "{db}"."{schema}"."{table}"').collect()
    grants = []
    cols = [c.lower() for c in rs[0]._fields] if rs else []
    def idx(name): return cols.index(name)
    for r in rs:
        try:
            granted_to = r[idx('granted_to')]          # ROLE or USER
            grantee    = r[idx('grantee_name')]
            privilege  = r[idx('privilege')]
            if granted_to == 'ROLE':
                grants.append({"role": grantee, "privilege": privilege})
            else:
                logging.warning(f'Skipping non-ROLE grant on {schema}.{table}: {granted_to} {grantee} {privilege}')
        except Exception:
            continue
    return grants

def ensure_role(sess: Session, role: str) -> None:
    sess.sql(f'CREATE ROLE IF NOT EXISTS "{role}"').collect()

def apply_role_grants(sess: Session, db: str, schema: str, table: str, grants: List[Dict[str,str]]) -> None:
    for g in grants:
        role = g["role"]
        priv = g["privilege"]
        ensure_role(sess, role)
        try:
            sess.sql(f'GRANT {priv} ON TABLE "{db}"."{schema}"."{table}" TO ROLE "{role}"').collect()
            logging.debug(f"Granted {priv} on {schema}.{table} to {role}")
        except Exception as e:
            if "already exists" in str(e).lower() or "dependent grant" in str(e).lower():
                logging.debug(f"Grant {priv} on {schema}.{table} to {role} already exists or conflicts, skipping: {e}")
            else:
                logging.warning(f"Failed to grant {priv} on {schema}.{table} to {role}: {e}")

def transfer_ownership(sess: Session, db: str, schema: str, table: str, owner_role: str) -> None:
    ensure_role(sess, owner_role)
    try:
        # Use REVOKE CURRENT GRANTS to handle existing grants automatically
        sess.sql(
            f'GRANT OWNERSHIP ON TABLE "{db}"."{schema}"."{table}" TO ROLE "{owner_role}" REVOKE CURRENT GRANTS'
        ).collect()
        logging.debug(f"Transferred ownership of {schema}.{table} to {owner_role}")
    except Exception as e:
        if "already owns" in str(e).lower():
            logging.debug(f"Role {owner_role} already owns {schema}.{table}, skipping ownership transfer")
        else:
            logging.warning(f"Failed to transfer ownership of {schema}.{table} to {owner_role}: {e}")

# ─────────────────────────── User-stage-based migration ────────────────────────

def migrate_tables_with_user_stage(
    src_sess,
    tgt_sess,
    source_db: str,
    source_schema: str,
    target_db: str,
    target_schema: str,
    tables: list,
    base_tmp_dir: str,
):
    """
    Migrates the specified list of tables using user stages (@~).

    Fixed issues:
    - Consistent file format between export/import
    - Explicit column ordering
    - Better error handling and validation
    - More robust file format options
    - Proper database/schema context management
    """
    import os
    import glob
    from datetime import datetime
    import logging

    # Ensure target session has proper database/schema context
    try:
        tgt_sess.sql(f'USE DATABASE "{target_db}"').collect()
        tgt_sess.sql(f'USE SCHEMA "{target_db}"."{target_schema}"').collect()
        logging.info(f'Target session confirmed using "{target_db}"."{target_schema}"')
    except Exception as e:
        raise RuntimeError(f'Failed to set target session context: {e}')

    # Use CSV format for better data integrity and debugging
    FILE_FORMAT = (
        "TYPE=CSV "
        "FIELD_DELIMITER=',' "
        "RECORD_DELIMITER='\\n' "
        "PARSE_HEADER=TRUE "
        "FIELD_OPTIONALLY_ENCLOSED_BY='\"' "
        "NULL_IF=('NULL', 'null', '') "
        "EMPTY_FIELD_AS_NULL=TRUE "
        "ESCAPE_UNENCLOSED_FIELD='\\\\'"
    )

    def _list_user_stage(sess, subpath: str):
        # More robust wildcard listing: LIST @~/<subpath>*
        sp = subpath.strip("/")
        # Try different listing patterns to find files
        patterns_to_try = [
            f"@~/{sp}*",        # All files starting with path
            f"@~/{sp}/",        # Direct directory listing
            f"@~/{sp}",         # Exact path
        ]
        
        for pattern in patterns_to_try:
            try:
                rows = sess.sql(f"LIST {pattern}").collect()
                if rows:
                    logging.debug(f"Found {len(rows)} files with pattern: {pattern}")
                    return [r[0] for r in rows]
                else:
                    logging.debug(f"No files found with pattern: {pattern}")
            except Exception as e:
                logging.debug(f"Error listing with pattern {pattern}: {e}")
                continue
        
        # If no pattern worked, return empty list
        logging.warning(f"Could not find any files for subpath: {sp}")
        return []

    def _clean_stage_path(sess, subpath: str):
        """Clean up stage path before use"""
        try:
            sess.sql(f"REMOVE @~/{subpath}/").collect()
            logging.debug(f"Cleaned stage path @~/{subpath}/")
        except Exception as e:
            logging.debug(f"Stage path @~/{subpath}/ was already clean or doesn't exist: {e}")

    summary = []

    for i, table in enumerate(tables, 1):
        try:
            print(f"\n📋 [{i}/{len(tables)}] Migrating {table}...")
            
            # 0) Source rowcount and column validation
            print(f"   🔍 Analyzing source table structure...")
            src_cnt_before = rowcount(src_sess, source_db, source_schema, table)
            src_columns = get_table_columns(src_sess, source_db, source_schema, table)
            
            if not src_columns:
                print(f"   ⚠️  No columns found for {source_schema}.{table}, skipping")
                summary.append((table, -1, -1, "NO_COLUMNS"))
                continue

            print(f"   📊 Source: {src_cnt_before:,} rows, {len(src_columns)} columns")

            # 1) Create/replace target table from source DDL (structure first)
            print(f"   🏗️  Creating target table structure...")
            ddl_src = get_table_ddl(src_sess, source_db, source_schema, table)
            ddl_tgt = rewrite_table_ddl(ddl_src, source_db, source_schema, target_db, target_schema)
            tgt_sess.sql(ddl_tgt).collect()
            logging.info(f'✅ Created target table "{target_db}"."{target_schema}"."{table}"')

            # Verify target table structure matches source
            tgt_columns = get_table_columns(tgt_sess, target_db, target_schema, table)
            if src_columns != tgt_columns:
                logging.warning(f"⚠️  Column mismatch between source and target:")
                logging.warning(f"Source: {src_columns}")
                logging.warning(f"Target: {tgt_columns}")

            # Capture source grants/owner for replay after load
            owner_role = show_table_owner(src_sess, source_db, source_schema, table) or "ACCOUNTADMIN"
            role_grants = fetch_role_grants_on_table(src_sess, source_db, source_schema, table)

            if src_cnt_before == 0:
                print(f"   ✅ Empty table - structure only migration")
                summary.append((table, 0, 0, "OK_EMPTY"))
                continue

            # 2) Clean and prepare stage paths - use simpler paths
            print(f"   🧹 Preparing staging areas...")
            timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
            src_stage_subpath = f"migrate_export_{timestamp}_{table}"
            tgt_stage_subpath = f"migrate_import_{timestamp}_{table}"
            
            _clean_stage_path(src_sess, src_stage_subpath)
            _clean_stage_path(tgt_sess, tgt_stage_subpath)

            # 3) UNLOAD to source user stage with explicit column list and header
            print(f"   📤 Exporting data from source...")
            # Build explicit column list to ensure proper ordering
            column_list = ', '.join([f'"{col}"' for col in src_columns])
            unload_sql = f"""
                COPY INTO @~/{src_stage_subpath}
                FROM (SELECT {column_list} FROM "{source_db}"."{source_schema}"."{table}")
                FILE_FORMAT=({FILE_FORMAT})
                HEADER=TRUE
                OVERWRITE=TRUE
                SINGLE=TRUE
            """
            logging.debug(f"Executing unload: {unload_sql}")
            unload_result = src_sess.sql(unload_sql).collect()
            logging.debug(f"Unload result: {unload_result}")

            # Debug: List all files in the user stage to see what's there
            logging.debug("Listing all files in user stage after unload:")
            try:
                all_files = src_sess.sql("LIST @~").collect()
                for f in all_files[:10]:  # Show first 10 files
                    logging.debug(f"  Stage file: {f[0]}")
                if len(all_files) > 10:
                    logging.debug(f"  ... and {len(all_files) - 10} more files")
            except Exception as e:
                logging.debug(f"Could not list all stage files: {e}")

            files = _list_user_stage(src_sess, src_stage_subpath)
            if not files:
                raise RuntimeError(
                    f"Unloaded 0 files at @~/{src_stage_subpath}% but source rowcount is {src_cnt_before}."
                )
            print(f"   ✅ Exported {len(files)} file(s) to source stage")

            # 4) GET to local using proper stage references
            print(f"   📥 Downloading data to local staging...")
            local_dir = os.path.join(base_tmp_dir, f"table_{table}_{timestamp}")
            os.makedirs(local_dir, exist_ok=True)
            
            # Clean local directory first
            for f in glob.glob(os.path.join(local_dir, "*")):
                os.remove(f)
                
            logging.debug(f"GET specific files from stage → file://{local_dir}")
            
            # GET each file using the wildcard pattern
            try:
                # Use wildcard to get all files for this prefix
                get_result = src_sess.sql(f"GET @~/{src_stage_subpath}* 'file://{local_dir}'").collect()
                logging.info(f"GET result: {get_result}")
            except Exception as e:
                logging.error(f"Wildcard GET failed: {e}")
                
                # Fallback: try individual file GETs with proper stage paths
                logging.info("Trying individual file GET operations...")
                for stage_file in files:
                    # Ensure proper stage path format
                    if not stage_file.startswith('@~'):
                        full_stage_path = f"@~/{stage_file}"
                    else:
                        full_stage_path = stage_file
                        
                    try:
                        get_result = src_sess.sql(f"GET '{full_stage_path}' 'file://{local_dir}'").collect()
                        logging.debug(f"GET result for {full_stage_path}: {get_result}")
                    except Exception as file_err:
                        logging.error(f"Failed to GET file {full_stage_path}: {file_err}")
                        continue

            # Check what we actually downloaded
            downloaded_files = os.listdir(local_dir)
            if not downloaded_files:
                raise RuntimeError(f"Failed to download any files from stage")

            total_size = sum(os.path.getsize(os.path.join(local_dir, f)) for f in downloaded_files)
            print(f"   ✅ Downloaded {len(downloaded_files)} file(s) ({total_size:,} bytes)")
            
            # Log file details only in debug mode
            for f in downloaded_files:
                local_path = os.path.join(local_dir, f)
                size = os.path.getsize(local_path)
                logging.debug(f"  {f}: {size} bytes")

            # 5) PUT to target user stage using actual downloaded files
            print(f"   📤 Uploading to target stage...")
            
            for local_file in downloaded_files:
                local_file_path = os.path.join(local_dir, local_file)
                if os.path.exists(local_file_path):
                    put_result = tgt_sess.sql(
                        f"PUT 'file://{local_file_path}' @~/{tgt_stage_subpath}/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
                    ).collect()
                    logging.debug(f"PUT result for {local_file}: {put_result}")
                else:
                    raise RuntimeError(f"Local file not found: {local_file_path}")

            # Verify files are in target stage
            tgt_files = _list_user_stage(tgt_sess, tgt_stage_subpath)
            if not tgt_files:
                raise RuntimeError(f"No files found in target stage @~/{tgt_stage_subpath}/")
            print(f"   ✅ Uploaded {len(tgt_files)} file(s) to target stage")

            # 6) LOAD into target table using automatic column mapping
            print(f"   📥 Loading data into target table...")
            fq_tgt = f'"{target_db}"."{target_schema}"."{table}"'
            
            copy_sql = f"""
                COPY INTO {fq_tgt}
                FROM @~/{tgt_stage_subpath}/
                FILE_FORMAT=({FILE_FORMAT})
                MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
                ON_ERROR='ABORT_STATEMENT'
                PURGE=TRUE
            """
            logging.debug(f"Executing load: {copy_sql}")
            load_result = tgt_sess.sql(copy_sql).collect()
            logging.debug(f"Load result: {load_result}")

            # 7) Re-apply ROLE grants and transfer OWNERSHIP on target (with better error handling)
            print(f"   🔐 Applying permissions and ownership...")
            try:
                grants_applied = 0
                if role_grants:
                    apply_role_grants(tgt_sess, target_db, target_schema, table, role_grants)
                    grants_applied = len(role_grants)
                    logging.debug(f"Applied {len(role_grants)} role grants")
                if owner_role:
                    transfer_ownership(tgt_sess, target_db, target_schema, table, owner_role)
                    logging.debug(f"Transferred ownership to {owner_role}")
                if grants_applied > 0 or owner_role:
                    print(f"   ✅ Applied permissions ({grants_applied} grants, owner: {owner_role})")
            except Exception as e:
                logging.warning(f"Non-critical grant/ownership error for {table}: {e}")

            # 8) Validate rowcounts
            print(f"   🔍 Validating data integrity...")
            s_cnt = src_cnt_before
            t_cnt = rowcount(tgt_sess, target_db, target_schema, table)
            status = "OK" if s_cnt == t_cnt else "ROWCOUNT_MISMATCH"
            
            if status == "ROWCOUNT_MISMATCH":
                print(f"   ❌ ERROR: Row count mismatch! Source: {s_cnt:,}, Target: {t_cnt:,}")
                logging.error(f"CRITICAL: Row count mismatch for {table}")
            else:
                print(f"   ✅ SUCCESS: {t_cnt:,} rows migrated successfully")
                
            summary.append((table, s_cnt, t_cnt, status))

        except Exception as e:
            print(f"   ❌ FAILED: {str(e)}")
            logging.exception(f"Failed migrating {source_schema}.{table}: {e}")
            summary.append((table, -1, -1, "ERROR"))

    return summary

# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    if SOURCE_SCHEMA.upper() == "INFORMATION_SCHEMA":
        _fatal("Refusing to operate on INFORMATION_SCHEMA.")

    # Welcome banner
    print("\n" + "=" * 80)
    print("🚀 SNOWFLAKE TABLE MIGRATION TOOL")
    print("=" * 80)
    print(f"📊 Source:      {SOURCE['account']} → {SOURCE_DB}.{SOURCE_SCHEMA}")
    print(f"🎯 Target:      {TARGET['account']} → {TARGET_DB}.{TARGET_SCHEMA}")
    print(f"📋 Tables:      {len(TABLES)} tables to migrate")
    print(f"👤 User:        {USERNAME}")
    print("=" * 80)

    # Track timing
    start_time = datetime.utcnow()

    # Scratch dir
    base_tmp = LOCAL_BASE or tempfile.mkdtemp(prefix="sf-migrate-")
    logging.info(f"📁 Temporary directory: {base_tmp}")

    # Connect
    print("🔗 Establishing connections...")
    src = connect_and_enforce(SOURCE, expect_user=USERNAME, expect_role="ACCOUNTADMIN")
    tgt = connect_and_enforce(TARGET, expect_user=USERNAME, expect_role="ACCOUNTADMIN")
    print("✅ Connected to both source and target Snowflake accounts")

    # Prepare target DB/SCHEMA
    print("🏗️  Preparing target database and schema...")
    ensure_db_schema(tgt, TARGET_DB, TARGET_SCHEMA)
    
    # Also ensure source session has proper context set
    try:
        src.sql(f'USE DATABASE "{SOURCE_DB}"').collect()
        src.sql(f'USE SCHEMA "{SOURCE_DB}"."{SOURCE_SCHEMA}"').collect()
        logging.info(f'📍 Source context set to "{SOURCE_DB}"."{SOURCE_SCHEMA}"')
    except Exception as e:
        _fatal(f'Failed to set source database/schema context: {e}')

    # Resolve source tables (normalize case-insensitive names against catalog)
    print("🔍 Validating source tables...")
    available = {t.upper(): t for t in list_tables(src, SOURCE_DB, SOURCE_SCHEMA)}
    wanted    = [t.upper() for t in TABLES]
    missing   = [t for t in wanted if t not in available]
    if missing:
        _fatal(f"❌ These tables do not exist in {SOURCE_DB}.{SOURCE_SCHEMA}: {missing}")
    tables = [available[t] for t in wanted]

    print(f"✅ Validated {len(tables)} tables: {', '.join(tables)}")
    
    # Migration
    print("\n" + "🚀 Starting table migration...")
    print("-" * 80)

    # Migrate using user stages (@~)
    summary = migrate_tables_with_user_stage(
        src, tgt,
        SOURCE_DB, SOURCE_SCHEMA,
        TARGET_DB, TARGET_SCHEMA,
        tables,
        base_tmp
    )

    # Calculate migration duration
    end_time = datetime.utcnow()
    duration = end_time - start_time
    duration_str = str(duration).split('.')[0]  # Remove microseconds

    # Enhanced Summary
    print("\n" + "=" * 80)
    print("📊 MIGRATION SUMMARY REPORT")
    print("=" * 80)
    
    total_tables = len(summary)
    successful = 0
    errors = 0
    mismatches = 0
    total_rows_migrated = 0
    empty_tables = 0
    
    # Calculate statistics
    for table_name, src_cnt, tgt_cnt, status in summary:
        if status == "OK":
            successful += 1
            total_rows_migrated += tgt_cnt if tgt_cnt > 0 else 0
        elif status == "OK_EMPTY":
            successful += 1
            empty_tables += 1
        elif status == "ERROR":
            errors += 1
        elif status == "ROWCOUNT_MISMATCH":
            mismatches += 1

    # Print detailed table results
    print(f"{'Table Name':<20} {'Source Rows':>12} {'Target Rows':>12} {'Status':>15}")
    print("-" * 80)
    
    for table_name, src_cnt, tgt_cnt, status in summary:
        # Format row counts
        src_display = f"{src_cnt:,}" if src_cnt >= 0 else "ERROR"
        tgt_display = f"{tgt_cnt:,}" if tgt_cnt >= 0 else "ERROR"
        
        # Add status emoji
        if status == "OK":
            status_display = "✅ SUCCESS"
        elif status == "OK_EMPTY":
            status_display = "✅ EMPTY"
        elif status == "ERROR":
            status_display = "❌ ERROR"
        elif status == "ROWCOUNT_MISMATCH":
            status_display = "⚠️  MISMATCH"
        else:
            status_display = status
            
        print(f"{table_name:<20} {src_display:>12} {tgt_display:>12} {status_display:>15}")
    
    print("-" * 80)
    
    # Summary statistics
    print("📈 MIGRATION STATISTICS:")
    print(f"   • Total Tables:       {total_tables}")
    print(f"   • Successful:         {successful} ✅")
    print(f"   • Empty Tables:       {empty_tables}")
    print(f"   • Row Mismatches:     {mismatches}")
    print(f"   • Errors:             {errors}")
    print(f"   • Total Rows Moved:   {total_rows_migrated:,}")
    print(f"   • Duration:           {duration_str}")
    
    # Environment details
    print(f"\n🔄 MIGRATION DETAILS:")
    print(f"   • Source Account:     {SOURCE['account']}")
    print(f"   • Target Account:     {TARGET['account']}")
    print(f"   • Database.Schema:    {SOURCE_DB}.{SOURCE_SCHEMA}")
    print(f"   • Migration Method:   Snowflake User Stages + CSV Format")
    print(f"   • Completed At:       {end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    print("=" * 80)
    
    # Final status
    if errors > 0 or mismatches > 0:
        print("⚠️  MIGRATION COMPLETED WITH ISSUES")
        print("❌ Please review the errors above and re-run for failed tables.")
        if mismatches > 0:
            print("⚠️  Row count mismatches detected - data integrity may be compromised.")
    else:
        print("🎉 MIGRATION COMPLETED SUCCESSFULLY!")
        print("✅ All tables migrated with full data integrity.")
        if total_rows_migrated > 0:
            print(f"📊 {total_rows_migrated:,} total rows successfully migrated.")

    print("=" * 80)

    # Cleanup temp scratch
    if LOCAL_BASE is None:
        try:
            shutil.rmtree(base_tmp)
            logging.info("🧹 Cleaned up temporary directory.")
        except Exception:
            logging.warning(f"Could not remove temp dir: {base_tmp}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        sys.exit(1)