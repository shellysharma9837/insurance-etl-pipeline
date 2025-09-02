import logging
import pandas as pd
from datetime import datetime, timezone
import json

from db import PostgresConnection
from config import CSV_FILE_PATH, TABLE_COLUMN_MAP
from utils import convert_boolean, schema_check

class InsuranceClaimsETL:
    def __init__(self):
        logging.basicConfig(
            filename=f"etl_info_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            level=logging.INFO
        )
        self.df = pd.read_csv(CSV_FILE_PATH)
        self.total_rows = len(self.df)
        self.anomalies = []
        self.processed_rows = 0
        self.successful_rows = 0
        self.failed_rows = 0

    def run(self):
        with PostgresConnection() as cur:
            load_start = datetime.now(timezone.utc)
            # Insert audit start record
            cur.execute("""
                INSERT INTO etl_load_audit(source_file_name, load_start_time, load_status) 
                VALUES (%s, %s, %s) RETURNING id
            """, (CSV_FILE_PATH, load_start, 'RUNNING'))
            audit_id = cur.fetchone()[0]

            logging.info(f"Load started at : {load_start}, total rows: {self.total_rows}")
            # Truncate tables
            cur.execute("TRUNCATE TABLE incidents, policies, customers RESTART IDENTITY CASCADE;")
            logging.info("Truncated incidents, policies, customers tables")

            # Schema check
            for table_name in TABLE_COLUMN_MAP.keys():
                schema_check(cur, table_name, TABLE_COLUMN_MAP)

            for idx, row in self.df.iterrows():
                self.processed_rows += 1
                try:
                    self.validate_row(row, idx)
                    customer_id = self.insert_customer(cur, row)
                    self.insert_policy(cur, row, customer_id)
                    self.insert_incident(cur, row)

                    self.successful_rows += 1
                except Exception as e:
                    self.failed_rows += 1
                    logging.error(f"Error processing row {idx}: {e}")
                    self.anomalies.append({"row": idx, "error": str(e)})
                    cur.connection.rollback()
            
            load_end = datetime.now(timezone.utc)
            load_status = 'SUCCESS' if self.failed_rows == 0 else 'FAILED'

            # Update audit log
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

            logging.info(f"Load ended at : {load_end}")
            logging.info(f"Processed: {self.processed_rows}, Success: {self.successful_rows}, Failed: {self.failed_rows}")

    def validate_row(self, row, idx):
        if pd.isnull(row['policy_number']):
            raise ValueError(f"Row {idx}: policy_number missing")
        if not (0 <= row['age'] <= 120):
            raise ValueError(f"Row {idx}: Invalid age: {row['age']}")
        if not isinstance(row['age'], (int, float)):
            raise ValueError(f"Row {idx}: Age not numeric: {row['age']}")
        if not pd.Series(self.df['policy_number']).is_unique:
            raise ValueError(f"Duplicate policy_number found")
        if row['insured_sex'] not in ['MALE', 'FEMALE']:
            raise ValueError(f"Row {idx}: Invalid insured_sex: {row['insured_sex']}")

    def insert_customer(self, cur, row):
        cur.execute("""
            INSERT INTO customers(months_as_customer, age, insured_sex, insured_education_level, 
                                  insured_occupation, insured_hobbies, insured_relationship, insured_zip)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING customer_id
        """, (
            row['months_as_customer'],
            row['age'], 
            row['insured_sex'],
            row['insured_education_level'],
            row['insured_occupation'],
            row['insured_hobbies'],
            row['insured_relationship'],
            row['insured_zip']
        ))
        return cur.fetchone()[0]

    def insert_policy(self, cur, row, customer_id):
        cur.execute("""
            INSERT INTO policies(policy_number, customer_id, policy_bind_date, policy_state, policy_csl,
                                 policy_deductable, policy_annual_premium, umbrella_limit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['policy_number'], 
            customer_id,
            row['policy_bind_date'],
            row['policy_state'],
            row['policy_csl'],
            row['policy_deductable'],
            row['policy_annual_premium'],
            row['umbrella_limit']
        ))

    def insert_incident(self, cur, row):
        cur.execute("""
            INSERT INTO incidents(policy_number, incident_date, incident_type, collision_type, incident_severity,
                                  authorities_contacted, incident_state, incident_city, incident_location, 
                                  incident_hour_of_the_day, number_of_vehicles_involved, property_damage, 
                                  bodily_injuries, witnesses, police_report_available, total_claim_amount, 
                                  injury_claim, property_claim, vehicle_claim, auto_make, auto_model,
                                  auto_year, fraud_reported)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['policy_number'], 
            row['incident_date'], 
            row['incident_type'], 
            row['collision_type'], 
            row['incident_severity'], 
            row['authorities_contacted'], 
            row['incident_state'], 
            row['incident_city'], 
            row['incident_location'], 
            row['incident_hour_of_the_day'],
            row['number_of_vehicles_involved'],
            convert_boolean(row['property_damage']),  # Using utility function for boolean conversion
            row['bodily_injuries'], 
            row['witnesses'], 
            convert_boolean(row['police_report_available']),
            row['total_claim_amount'], 
            row['injury_claim'], 
            row['property_claim'], 
            row['vehicle_claim'], 
            row['auto_make'], 
            row['auto_model'], 
            row['auto_year'], 
            convert_boolean(row['fraud_reported'])
        ))
