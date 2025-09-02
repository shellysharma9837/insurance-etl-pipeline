import pandas as pd
import logging
from config import boolean_cols

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

    missing_cols = expected_columns - db_columns
    extra_cols = db_columns - expected_columns

    if missing_cols:
        logging.warning(f"CSV expects columns not found in {table_name}: {missing_cols}")
    if extra_cols:
        logging.warning(f"Table {table_name} has extra columns not in CSV mapping: {extra_cols}")

def insert_row(cur, table_name, row, table_column_map, boolean_cols):
    columns = table_column_map[table_name]
    placeholders = ','.join(['%s'] * len(columns))
    columns_joined = ','.join(columns)
    sql = f"INSERT INTO {table_name}({columns_joined}) VALUES ({placeholders})"
    values = []
    for col in columns:
        val = row[col]
        # Handle booleans
        if col in boolean_cols:  
            val = convert_boolean(val)
        values.append(val)
    cur.execute(sql, values)

def insert_row_with_returning(cur, table_name, row, table_column_map, boolean_cols, returning_col=None):
    columns = table_column_map[table_name]
    placeholders = ','.join(['%s'] * len(columns))
    columns_joined = ','.join(columns)
    sql = f"INSERT INTO {table_name}({columns_joined}) VALUES ({placeholders})"
    if returning_col:
        sql += f" RETURNING {returning_col}"
    values = []
    for col in columns:
        val = row[col]
        # Handle booleans
        if col in boolean_cols:  
            val = convert_boolean(val)
        values.append(val)
    cur.execute(sql, values)
    return cur.fetchone()[0] if returning_col else None

def validate_row_generic(row, table_name, table_validations):
    rules = table_validations.get(table_name, {})
    for col, func in rules.items():
        if col in row and not func(row[col]):
            raise ValueError(f"Invalid value for {col}: {row[col]}")
