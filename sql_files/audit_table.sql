CREATE TABLE etl_load_audit (
    id SERIAL PRIMARY KEY,
    source_file_name TEXT NOT NULL,
    load_start_time TIMESTAMPTZ NOT NULL,
    load_end_time TIMESTAMPTZ,
    total_rows INT,
    successful_rows INT,
    failed_rows INT,
    anomalies JSONB,   -- Store anomaly details as JSON
    load_status TEXT,  -- e.g. 'RUNNING', 'SUCCESS', 'FAILED'
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    last_incident_date_loaded TIMESTAMPTZ NOT NULL,
    last_policy_date_loaded TIMESTAMPTZ NOT null,
);
