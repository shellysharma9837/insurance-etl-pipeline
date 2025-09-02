import logging
import pandas as pd
from datetime import datetime, timezone
import json

from db import PostgresConnection
from config import CSV_FILE_PATH, TABLE_COLUMN_MAP, TABLE_VALIDATIONS, boolean_cols
from utils import schema_check, insert_row_with_returning, insert_row, validate_row_generic  # use upsert_row instead of insert_row

class IncrementalETL:
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
            # 1. Get last successful load dates
            cur.execute("""
                SELECT 
                    COALESCE(MAX(last_incident_date_loaded), '1900-01-01') AS last_incident,
                    COALESCE(MAX(last_policy_date_loaded), '1900-01-01') AS last_policy
                FROM etl_load_audit
                WHERE load_status = 'SUCCESS'
            """)
            last_incident_date, last_policy_date = cur.fetchone()

            # 2. Filter new/updated rows
            try:
    # Normalize last load dates (set time = 00:00:00)
                #last_incident_date = pd.to_datetime(last_incident_date).normalize()
                #last_policy_date = pd.to_datetime(last_policy_date).normalize()

    # Convert and filter rows
                self.df['incident_date'] = pd.to_datetime(
                self.df['incident_date'], format='%d/%m/%Y', errors='coerce'
                )
                self.df['policy_bind_date'] = pd.to_datetime(
                self.df['policy_bind_date'], format='%d/%m/%Y', errors='coerce'
                )

                new_rows = self.df[
                (self.df['incident_date'] > last_incident_date) |
                (self.df['policy_bind_date'] > last_policy_date)
                ]
            except Exception as e:
                logging.error(f"Error filtering rows: {e}")
                new_rows = self.df.iloc[0:0] 

            load_start = datetime.now(timezone.utc)
            cur.execute("""
                INSERT INTO etl_load_audit(source_file_name, load_start_time, load_status)
                VALUES (%s, %s, %s) RETURNING id
            """, (CSV_FILE_PATH, load_start, 'RUNNING'))
            audit_id = cur.fetchone()[0]
            cur.connection.commit()

            logging.info(f"Incremental load started at: {load_start}, rows to process: {len(new_rows)}")

            # Schema check (optional)
            for table in TABLE_COLUMN_MAP.keys():
                schema_check(cur, table, TABLE_COLUMN_MAP)

            # Process rows
            for idx, row in new_rows.iterrows():
                self.processed_rows += 1
                try:
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

            last_incident_date_loaded = (
    pd.to_datetime(new_rows['incident_date'], format='%d/%m/%Y', errors='coerce').max()
    if not new_rows.empty else last_incident_date
)           
            last_policy_date_loaded = (
    pd.to_datetime(new_rows['policy_bind_date'], format='%d/%m/%Y', errors='coerce').max()
    if not new_rows.empty else last_policy_date
)
            # Update audit table
            load_end = datetime.now(timezone.utc)
            load_status = 'SUCCESS' if self.failed_rows == 0 else 'FAILED'
            cur.execute("""
                UPDATE etl_load_audit
                SET load_end_time = %s,
                    total_rows = %s,
                    successful_rows = %s,
                    failed_rows = %s,
                    anomalies = %s,
                    load_status = %s,
                    last_incident_date_loaded = %s,
                    last_policy_date_loaded = %s
                WHERE id = %s
            """, (
                load_end, len(new_rows), self.successful_rows, self.failed_rows,
                json.dumps(self.anomalies), load_status,
                last_incident_date_loaded,
                last_policy_date_loaded,
                audit_id
            ))

            logging.info(f"Incremental load ended at: {load_end}")
            logging.info(f"Processed: {self.processed_rows}, Success: {self.successful_rows}, Failed: {self.failed_rows}")
