from sqlalchemy import text
from database.db_connection import get_engine
from utils.logger import get_logger

log = get_logger("run_schema")

def run_schema(path="database/schema.sql"):
    engine = get_engine()
    with open(path) as f:
        sql = f.read()
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    log.info("Schema created successfully.")

if __name__ == "__main__":
    run_schema()