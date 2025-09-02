-- Drop existing tables
--DROP TABLE IF EXISTS incidents CASCADE;
--DROP TABLE IF EXISTS policies CASCADE;
-- TABLE IF EXISTS customers CASCADE;

-- Create customers table (EXACT CSV column names)
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    months_as_customer INTEGER,
    age INTEGER,
    insured_sex VARCHAR(10),
    insured_education_level VARCHAR(50),
    insured_occupation VARCHAR(100),
    insured_hobbies TEXT,
    insured_relationship VARCHAR(50),
    insured_zip VARCHAR(10)
);

-- Create policies table (EXACT CSV column names)
CREATE TABLE policies (
    policy_number VARCHAR(20) PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    policy_bind_date DATE,
    policy_state VARCHAR(10),
    policy_csl VARCHAR(20),
    policy_deductable NUMERIC(10,2),
    policy_annual_premium NUMERIC(10,2),
    umbrella_limit NUMERIC(10,2)
);

-- Create incidents table (EXACT CSV column names)
CREATE TABLE incidents (
    incident_id SERIAL PRIMARY KEY,
    policy_number VARCHAR(20) REFERENCES policies(policy_number),
    incident_date DATE,
    incident_type VARCHAR(50),
    collision_type VARCHAR(50),
    incident_severity VARCHAR(20),
    authorities_contacted VARCHAR(50),
    incident_state VARCHAR(10),
    incident_city VARCHAR(100),
    incident_location TEXT,
    incident_hour_of_the_day INTEGER,
    number_of_vehicles_involved INTEGER,
    property_damage BOOLEAN,
    bodily_injuries INTEGER,
    witnesses INTEGER,
    police_report_available BOOLEAN,
    total_claim_amount NUMERIC(12,2),
    injury_claim NUMERIC(12,2),
    property_claim NUMERIC(12,2),
    vehicle_claim NUMERIC(12,2),
    auto_make VARCHAR(50),
    auto_model VARCHAR(50),
    auto_year INTEGER,
    fraud_reported BOOLEAN
);