# PySpark ETL: PostgreSQL to SQL Server
A production-ready data migration pipeline to move 500k+ records from PostgreSQL to SQL Server using distributed PySpark processing.

## Setup
1. Copy `.env.example` to `.env` and fill in your passwords.
2. Update `config/settings.yaml` with your database credentials.
3. Place JDBC jars in the `jars/` folder.
4. Run `sh scripts/submit.sh`.
