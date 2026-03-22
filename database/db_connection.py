import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from utils.logger import get_logger

load_dotenv()
log = get_logger("db_connection")

def get_engine():
    host     = os.getenv("DB_HOST", "localhost")
    port     = os.getenv("DB_PORT", "5432")
    dbname   = os.getenv("DB_NAME", "nav_platform")
    user     = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres123")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    try:
        engine = create_engine(url, echo=False, pool_pre_ping=True)
        log.info(f"Database engine created: {host}:{port}/{dbname}")
        return engine
    except Exception as e:
        log.error(f"Failed to create engine: {e}")
        raise