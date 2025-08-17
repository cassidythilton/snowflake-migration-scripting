# Snowflake Migration Scripting

🚀 **Professional-grade Snowflake database migration tool for seamlessly transferring tables between Snowflake accounts.**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Compatible-29B5E8.svg)](https://www.snowflake.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 📋 Overview

This repository contains a robust Python script for migrating tables, data, and metadata between different Snowflake accounts. Perfect for:

- **Trial-to-Production migrations**
- **Account consolidation**
- **Cross-region data transfers**
- **Development environment setup**
- **Disaster recovery scenarios**

## ✨ Key Features

- 🔐 **Secure JWT Authentication** - Uses RSA key pairs for secure connection
- 🏗️ **Complete Structure Migration** - Preserves table schemas, data types, and constraints
- 📊 **Data Integrity Validation** - Automatic row count verification and data quality checks
- 🎯 **Smart Column Mapping** - Intelligent column matching with case-insensitive headers
- 🔄 **Idempotent Operations** - Safe to re-run for failed tables
- 📈 **Progress Tracking** - Real-time migration progress with detailed logging
- 🛡️ **Permission Preservation** - Maintains original table ownership and grants
- 🧹 **Automatic Cleanup** - Temporary files and staging areas cleaned automatically
- 📊 **Comprehensive Reporting** - Detailed migration summary with statistics and timing

## 🛠️ Requirements

### System Requirements
- **Python 3.8+** (tested with Python 3.11+)
- **snowflake-snowpark-python >= 1.15.0**
- **RSA private key** for JWT authentication
- **Network access** to both source and target Snowflake accounts

### Snowflake Requirements
- **ACCOUNTADMIN role** in both accounts (or equivalent privileges)
- **COMPUTE_WH warehouse** (or any accessible warehouse)
- **User stages enabled** for data transfer
- **Sufficient storage quota** for temporary staging

## 📦 Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/cassidythilton/snowflake-migration-scripting.git
   cd snowflake-migration-scripting
   ```

2. **Install dependencies:**
   ```bash
   pip install snowflake-snowpark-python
   ```

## 🔑 Authentication Setup

### 1. Generate RSA Key Pair

```bash
# Generate private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Generate public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

### 2. Configure Snowflake User

Upload the public key to your Snowflake user account:

```sql
-- Copy the content of rsa_key.pub (without BEGIN/END headers)
ALTER USER <your_username> SET RSA_PUBLIC_KEY='<public_key_content>';
```

### 3. Secure Your Private Key

```bash
# Set appropriate permissions
chmod 600 rsa_key.p8

# Store in a secure location
mv rsa_key.p8 ~/.ssh/snowflake_rsa_key.p8
```

## ⚙️ Configuration

Edit the **USER CONFIG** section in `snowflakeMig.py`:

```python
# Your Snowflake username (same in both accounts)
USERNAME = "your_username"

# Source Snowflake account
SOURCE = {
    "account": "ABC12345-XY67890",  # Your source account identifier
    "user": "your_username",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "authenticator": "SNOWFLAKE_JWT",
    "private_key_file": "/path/to/your/rsa_key.p8",
}

# Target Snowflake account
TARGET = {
    "account": "DEF67890-ZW12345",  # Your target account identifier
    "user": "your_username",
    "role": "ACCOUNTADMIN", 
    "warehouse": "COMPUTE_WH",
    "authenticator": "SNOWFLAKE_JWT",
    "private_key_file": "/path/to/your/rsa_key.p8",
}

# Migration scope
SOURCE_DB = "your_source_database"
SOURCE_SCHEMA = "your_source_schema"
TABLES = [
    "table1",
    "table2", 
    "table3",
    # Add your tables here
]

# Target destination (can be same or different names)
TARGET_DB = "your_target_database"
TARGET_SCHEMA = "your_target_schema"
```

## 🚀 Usage

### Basic Migration

```bash
python3 snowflakeMig.py
```

### With Debug Logging

For troubleshooting, enable debug mode by setting:

```python
LOG_LEVEL = logging.DEBUG
```

### Utility Scripts

The repository includes additional utility scripts for testing and validation:

#### 🔍 Source Data Inspector
```bash
python3 check_source_data.py
```
- Inspects source tables before migration
- Verifies data accessibility and samples content
- Useful for pre-migration validation

#### 🔍 Data Quality Checker  
```bash
python3 check_data_quality.py
```
- Analyzes target tables after migration
- Checks for NULL values and data integrity issues
- Useful for post-migration validation

### Expected Output

```
================================================================================
🚀 SNOWFLAKE TABLE MIGRATION TOOL
================================================================================
📊 Source:      ABC12345-XY67890 → SOURCE_DB.SOURCE_SCHEMA
🎯 Target:      DEF67890-ZW12345 → TARGET_DB.TARGET_SCHEMA
📋 Tables:      4 tables to migrate
👤 User:        your_username
================================================================================

🔗 Establishing connections...
✅ Connected to both source and target Snowflake accounts
🏗️  Preparing target database and schema...
🔍 Validating source tables...
✅ Validated 4 tables: EMPLOYEES, CUSTOMERS, ORDERS, PRODUCTS

🚀 Starting table migration...
--------------------------------------------------------------------------------

📋 [1/4] Migrating EMPLOYEES...
   🔍 Analyzing source table structure...
   📊 Source: 17 rows, 12 columns
   🏗️  Creating target table structure...
   🧹 Preparing staging areas...
   📤 Exporting data from source...
   📥 Downloading data to local staging...
   📤 Uploading to target stage...
   📥 Loading data into target table...
   🔐 Applying permissions and ownership...
   🔍 Validating data integrity...
   ✅ SUCCESS: 17 rows migrated successfully

================================================================================
📊 MIGRATION SUMMARY REPORT
================================================================================
Table Name            Source Rows  Target Rows          Status
--------------------------------------------------------------------------------
EMPLOYEES                      17           17       ✅ SUCCESS
CUSTOMERS                     640          640       ✅ SUCCESS
ORDERS                     51,075       51,075       ✅ SUCCESS
PRODUCTS                      295          295       ✅ SUCCESS
--------------------------------------------------------------------------------
📈 MIGRATION STATISTICS:
   • Total Tables:       4
   • Successful:         4 ✅
   • Empty Tables:       0
   • Row Mismatches:     0
   • Errors:             0
   • Total Rows Moved:   52,027
   • Duration:           0:00:43

🎉 MIGRATION COMPLETED SUCCESSFULLY!
✅ All tables migrated with full data integrity.
📊 52,027 total rows successfully migrated.
================================================================================
```

## 🔄 Migration Process

The script follows a comprehensive 8-step process for each table:

1. **🔍 Analysis** - Examines source table structure and row count
2. **🏗️ Structure Creation** - Creates target table with identical schema
3. **📤 Data Export** - Exports source data to staging area (CSV format)
4. **📥 Local Download** - Downloads data to temporary local directory
5. **📤 Target Upload** - Uploads data to target account staging area
6. **📥 Data Loading** - Loads data with automatic column mapping
7. **🔐 Permission Application** - Applies original ownership and grants
8. **🔍 Validation** - Verifies row counts and data integrity

## 🛡️ Security & Best Practices

### Security Features
- ✅ **JWT Authentication** - No password storage required
- ✅ **Encrypted Connections** - All data transfer uses TLS encryption
- ✅ **Temporary Storage** - Local files automatically cleaned up
- ✅ **Permission Preservation** - Original access controls maintained

### Best Practices
- 🔒 Store private keys securely with restricted permissions
- 📋 Test migrations on small tables first
- 🕐 Run during low-usage periods for large tables
- 📊 Monitor Snowflake credit consumption during migration
- 💾 Backup important data before migration

## 🐛 Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| **Authentication Failed** | Verify RSA public key is uploaded to Snowflake user |
| **Permission Denied** | Ensure user has ACCOUNTADMIN role or sufficient privileges |
| **Table Not Found** | Check table names in TABLES list (case-insensitive) |
| **Network Timeout** | Verify network connectivity to both Snowflake accounts |
| **Stage Quota Exceeded** | Clean up user stages or increase storage quota |

### Debug Mode

Enable detailed logging for troubleshooting:

```python
LOG_LEVEL = logging.DEBUG
```

This provides verbose output including:
- SQL statements executed
- File transfer details
- Stage operations
- Error stack traces

### Getting Help

If you encounter issues:

1. **Check the logs** - Enable DEBUG mode for detailed information
2. **Verify configuration** - Double-check account identifiers and paths
3. **Test connectivity** - Ensure both accounts are accessible
4. **Review permissions** - Confirm user has necessary privileges
5. **Open an issue** - [Create a GitHub issue](https://github.com/cassidythilton/snowflake-migration-scripting/issues) with logs

## 📊 Performance

### Benchmarks

Based on testing with various table sizes:

| Table Size | Migration Time | Throughput |
|------------|----------------|------------|
| 1K rows | ~5 seconds | ~200 rows/sec |
| 100K rows | ~30 seconds | ~3,333 rows/sec |
| 1M rows | ~3 minutes | ~5,556 rows/sec |
| 10M rows | ~25 minutes | ~6,667 rows/sec |

*Performance varies based on data types, network speed, and Snowflake warehouse size.*

### Optimization Tips

- 🏎️ **Use larger warehouses** for faster data processing
- 📊 **Monitor credit consumption** vs. time savings
- 🔄 **Run parallel migrations** for independent table sets
- 📦 **Compress large text fields** before migration if possible

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

### Development Setup

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes
4. Test thoroughly with your Snowflake accounts
5. Commit your changes: `git commit -m 'Add amazing feature'`
6. Push to the branch: `git push origin feature/amazing-feature`
7. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙋‍♂️ Support

- 📧 **Email**: [Create an issue](https://github.com/cassidythilton/snowflake-migration-scripting/issues)
- 📖 **Documentation**: See comments in `snowflakeMig.py`
- 🐛 **Bug Reports**: [GitHub Issues](https://github.com/cassidythilton/snowflake-migration-scripting/issues)

## 🏷️ Version History

- **v1.0.0** - Initial release with complete migration functionality
  - JWT authentication support
  - Full table structure migration
  - Data integrity validation
  - Permission preservation
  - Comprehensive logging and reporting

---

**⭐ If this tool helps you, please give it a star! It helps others discover the project.**

Built with ❤️ by [Cassidy Hilton](https://github.com/cassidythilton)
