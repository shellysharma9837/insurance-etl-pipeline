# Insurance ETL Pipeline

This repository contains a Python-based ETL pipeline designed for **loading insurance data** from CSV files into a PostgreSQL database. The pipeline supports **full load and incremental load** processes with schema validation, row-level auditing, and anomaly logging.

---

## Project Structure

insurance_etl/
│
├── etl/
│ ├── incremental_etl.py # Incremental ETL logic
│ ├── full_load_etl.py # Full load ETL logic
│ ├── db.py # PostgreSQL connection utility
│ ├── config.py # Configuration: CSV paths, table mappings, validations
│ └── utils.py # Utility functions: schema_check, row validation, insert/upsert
│
├── logs/ # ETL execution logs
├── data/ # Sample CSV files
├── README.md
└── requirements.txt # Python dependencies


---

## Code Details

### **IncrementalETL class (`incremental_etl.py`)**

- Reads CSV files into a Pandas DataFrame.
- Determines new or updated rows based on the last successful load (`incident_date` and `policy_bind_date`).
- Performs **schema checks** on database tables.
- Validates rows using predefined rules (`TABLE_VALIDATIONS`) and boolean column handling.
- Inserts rows into multiple related tables (`customers` first, then dependent tables).
- Tracks anomalies and failed rows; logs detailed error messages.
- Updates an **audit table (`etl_load_audit`)** with:
  - Load start/end timestamps  
  - Total, successful, and failed row counts  
  - Row-level anomalies  
  - Last successfully loaded incident and policy dates  

---

### **Full Load ETL (`full_load_etl.py`)**

- Loads the entire CSV into the database.
- Performs the same schema checks, row validations, and logging as incremental ETL.
- Useful for initializing tables or reprocessing all data.

---

### **Database Utility (`db.py`)**

- Provides a context-managed PostgreSQL connection (`PostgresConnection`).
- Ensures proper commit/rollback handling for ETL operations.

---

### **Configuration (`config.py`)**

- `CSV_FILE_PATH`: Path to the CSV file for ETL.
- `TABLE_COLUMN_MAP`: Maps DataFrame columns to database table columns.
- `TABLE_VALIDATIONS`: Rules for row validation.
- `boolean_cols`: Columns treated as boolean.

---

### **Utility Functions (`utils.py`)**

- `schema_check`: Validates that required columns exist in database tables.
- `insert_row_with_returning`: Inserts a row and returns the primary key.
- `insert_row` / `upsert_row`: Inserts or updates rows in tables.
- `validate_row_generic`: Validates a row against defined rules.

---

### **Logging and Audit**

- Logs are generated for each ETL run in the `logs/` folder.
- Each incremental load records:
  - Start and end time
  - Number of rows processed, successful, and failed
  - Anomalies with row-level errors
  - Audit table updated with last loaded dates

---

### **Key Features**

- Supports **full and incremental ETL** processes.
- Row-level **error handling** with rollback on failure.
- Maintains an **audit trail** of all ETL runs.
- Handles **date-based filtering** for incremental loads.
- Modular design for maintainability and scalability.

---

### **Dependencies**

- `pandas` – for data manipulation  
- `psycopg2` or `sqlalchemy` – for PostgreSQL connection  
- `logging` – for logging ETL operations  
- `json` – for anomaly tracking  
- Python >= 3.7
