import numpy as np
import pandas as pd
from sqlalchemy import text
from database.db_connection import get_engine
from utils.logger import get_logger

log = get_logger("inject_anomalies")

ANOMALY_PLAN = [
    (1,  "fat_finger",       0.10,  "NAV x10 decimal place error"),
    (2,  "fat_finger",      -0.90,  "NAV /10 decimal place error"),
    (3,  "stale_price",      0,     "5-day static NAV"),
    (4,  "stale_price",      0,     "7-day static NAV"),
    (5,  "fx_shock",        -0.15,  "15% drop FX misapplication"),
    (6,  "fx_shock",         0.12,  "12% spike FX misapplication"),
    (7,  "liquidity_event", -0.08,  "8% drop forced redemption"),
    (8,  "liquidity_event", -0.06,  "6% drop large investor exit"),
    (9,  "corporate_action", 0.05,  "5% spike dividend not stripped"),
    (10, "corporate_action",-0.07,  "7% drop rights issue mispriced"),
    (1,  "fx_shock",        -0.09,  "Secondary FX shock Fund 1"),
    (3,  "fat_finger",       0.10,  "Second fat-finger Fund 3"),
    (5,  "stale_price",      0,     "Stale pricing Fund 5"),
    (7,  "fx_shock",         0.08,  "FX shock Fund 7"),
    (2,  "liquidity_event", -0.05,  "Liquidity event Fund 2"),
    (4,  "corporate_action", 0.06,  "Corporate action Fund 4"),
    (6,  "stale_price",      0,     "Stale pricing Fund 6"),
    (8,  "fat_finger",       0.10,  "Fat-finger Fund 8"),
    (9,  "fx_shock",        -0.11,  "FX shock Fund 9"),
    (10, "stale_price",      0,     "Stale pricing Fund 10"),
]

def inject_anomalies():
    engine = get_engine()
    np.random.seed(99)

    with engine.connect() as conn:
        all_dates = pd.read_sql(
            "SELECT DISTINCT price_date FROM fact_nav_pricing ORDER BY price_date", conn
        )["price_date"].tolist()

    valid_dates  = all_dates[30:-30]
    selected_dates = np.random.choice(valid_dates, size=len(ANOMALY_PLAN), replace=False)

    with engine.connect() as conn:
        for i, (fund_id, atype, magnitude, desc) in enumerate(ANOMALY_PLAN):
            adate = selected_dates[i]
            row = conn.execute(text("""
                SELECT nav_id, nav_value FROM fact_nav_pricing
                WHERE fund_id = :f AND price_date = :d
            """), {"f": fund_id, "d": adate}).fetchone()
            if not row:
                log.warning(f"No row found for fund {fund_id} on {adate}")
                continue
            nav_id, nav_value = row

            if atype == "stale_price":
                stale_n = 5 if i % 2 == 0 else 7
                start_idx = all_dates.index(adate)
                stale_dates = all_dates[start_idx: start_idx + stale_n]
                for sd in stale_dates:
                    conn.execute(text("""
                        UPDATE fact_nav_pricing
                        SET nav_value = :v, daily_return = 0.0,
                            is_injected_anomaly = TRUE, anomaly_type = :t
                        WHERE fund_id = :f AND price_date = :d
                    """), {"v": nav_value, "t": atype, "f": fund_id, "d": sd})
            else:
                new_nav = round(nav_value * (1 + magnitude), 6)
                conn.execute(text("""
                    UPDATE fact_nav_pricing
                    SET nav_value = :v, daily_return = :r,
                        is_injected_anomaly = TRUE, anomaly_type = :t
                    WHERE nav_id = :nid
                """), {"v": new_nav, "r": magnitude, "t": atype, "nid": nav_id})

            log.info(f"Injected [{atype}] | Fund {fund_id} | {adate} | {desc}")
        conn.commit()

    log.info("All 20 anomalies injected.")

if __name__ == "__main__":
    inject_anomalies()