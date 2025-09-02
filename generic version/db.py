import psycopg2
from config import DATABASE_CONFIG

class PostgresConnection:
    def __init__(self, config=DATABASE_CONFIG):
        self.config = config
        self.conn = None
        self.cur = None

    def __enter__(self):
        self.conn = psycopg2.connect(**self.config)
        self.cur = self.conn.cursor()
        return self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.cur.close()
        self.conn.close()
