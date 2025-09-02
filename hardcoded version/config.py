DATABASE_CONFIG = {
    "host": "localhost",
    "port": "5433",
    "dbname": "postgres",
    "user": "postgres",
    "password": "Takecare@2025"
}

CSV_FILE_PATH = '/Users/shellysharma/Downloads/insurance_claims.csv'

TABLE_COLUMN_MAP = {
    'customers': ['months_as_customer', 'age', 'insured_sex', 'insured_education_level',
                  'insured_occupation', 'insured_hobbies', 'insured_relationship', 'insured_zip'],
    'policies': ['policy_number', 'policy_bind_date', 'policy_state', 'policy_csl',
                 'policy_deductable', 'policy_annual_premium', 'umbrella_limit'],
    'incidents': ['policy_number', 'incident_date', 'incident_type', 'collision_type', 'incident_severity',
                  'authorities_contacted', 'incident_state', 'incident_city', 'incident_location',
                  'incident_hour_of_the_day', 'number_of_vehicles_involved', 'property_damage',
                  'bodily_injuries', 'witnesses', 'police_report_available', 'total_claim_amount',
                  'injury_claim', 'property_claim', 'vehicle_claim', 'auto_make', 'auto_model',
                  'auto_year', 'fraud_reported']
}
