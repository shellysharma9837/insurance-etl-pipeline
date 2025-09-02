import pandas as pd
import logging

def convert_boolean(value):
    if pd.isna(value) or value == '?' or value == '':
        return None
    if str(value).lower() in ['yes', 'true', '1', 'y']:
        return True
    if str(value).lower() in ['no', 'false', '0', 'n']:
        return False
    return None

def schema_check(cur, table_name, table_column_map):
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s;
    """, (table_name,))
    db_columns = {row[0] for row in cur.fetchall()}
    expected_columns = set(table_column_map[table_name])

    missing_cols = db_columns - expected_columns
    extra_cols = expected_columns - db_columns

    if missing_cols:
        logging.warning(f"Table {table_name} has extra columns not in CSV mapping: {missing_cols}")
    if extra_cols:
        logging.warning(f"CSV expects columns not found in {table_name}: {extra_cols}")
