import logging
import pandas as pd
from datetime import datetime, timezone
import json

from db import PostgresConnection
from config import CSV_FILE_PATH, TABLE_COLUMN_MAP, TABLE_VALIDATIONS, boolean_cols
from utils import schema_check, insert_row, insert_row_with_returning, validate_row_generic

class GenericETL:
    def __init__(self, csv_path=CSV_FILE_PATH):
        logging.basicConfig(
            filename=f"etl_info_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            level=logging.INFO
        )
        self.df = pd.read_csv(csv_path)
        self.total_rows = len(self.df)
        self.anomalies = []
        self.processed_rows = 0
        self.successful_rows = 0
        self.failed_rows = 0

    def run(self):
        with PostgresConnection() as cur:
            load_start = datetime.now(timezone.utc)
            cur.execute("""
                INSERT INTO etl_load_audit(source_file_name, load_start_time, load_status) 
                VALUES (%s, %s, %s) RETURNING id
            """, (CSV_FILE_PATH, load_start, 'RUNNING'))
            audit_id = cur.fetchone()[0]
            cur.connection.commit()

            logging.info(f"Load started at: {load_start}, total rows: {self.total_rows}")

            # Truncate all tables
            for table in TABLE_COLUMN_MAP.keys():
                cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;")
                logging.info(f"Truncated {table} table")

            # Schema check
            for table in TABLE_COLUMN_MAP.keys():
                schema_check(cur, table, TABLE_COLUMN_MAP)

            # Process each row
            for idx, row in self.df.iterrows():
                self.processed_rows += 1
                try:
                    # Generic row validation
                    for table in TABLE_COLUMN_MAP.keys():
                        validate_row_generic(row, table, TABLE_VALIDATIONS)  

                    # Insert customers first and get customer_id
                    customer_id = insert_row_with_returning(cur, 'customers', row, TABLE_COLUMN_MAP, boolean_cols, 'customer_id')
                    row['customer_id'] = customer_id

                    # Insert other tables
                    for table in TABLE_COLUMN_MAP.keys():
                        if table != 'customers':
                            insert_row(cur, table, row, TABLE_COLUMN_MAP, boolean_cols)

                    self.successful_rows += 1

                except Exception as e:
                    self.failed_rows += 1
                    logging.error(f"Error processing row {idx}: {e}")
                    self.anomalies.append({"row": idx, "error": str(e)})
                    cur.connection.rollback()

            # Audit update
            load_end = datetime.now(timezone.utc)
            load_status = 'SUCCESS' if self.failed_rows == 0 else 'FAILED'
            cur.execute("""
                UPDATE etl_load_audit
                SET load_end_time = %s,
                    total_rows = %s,
                    successful_rows = %s,
                    failed_rows = %s,
                    anomalies = %s,
                    load_status = %s
                WHERE id = %s
            """, (load_end, self.total_rows, self.successful_rows, self.failed_rows,
                  json.dumps(self.anomalies), load_status, audit_id))

            logging.info(f"Load ended at: {load_end}")
            logging.info(f"Processed: {self.processed_rows}, Success: {self.successful_rows}, Failed: {self.failed_rows}")
