from sqlalchemy import text
from database.db_connection import get_engine
from utils.logger import get_logger

log = get_logger("reset_db")

def reset_database():
    engine = get_engine()
    log.warning("RESETTING DATABASE — all data will be deleted.")
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS fact_anomalies CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS fact_nav_pricing CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS dim_fund CASCADE"))
        conn.execute(text("DROP VIEW IF EXISTS v_anomaly_dashboard CASCADE"))
        conn.execute(text("DROP VIEW IF EXISTS v_cssf_breach_summary CASCADE"))
        conn.execute(text("DROP VIEW IF EXISTS v_monthly_trend CASCADE"))
        conn.execute(text("DROP VIEW IF EXISTS v_nav_summary CASCADE"))
        conn.commit()
    log.info("All tables dropped. Run run_pipeline.py to rebuild.")

if __name__ == "__main__":
    confirm = input("Type RESET to confirm database wipe: ")
    if confirm == "RESET":
        reset_database()
    else:
        print("Cancelled.")