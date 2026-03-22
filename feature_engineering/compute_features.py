import pandas as pd
import numpy as np
from sqlalchemy import text
from database.db_connection import get_engine
from utils.logger import get_logger

log = get_logger("compute_features")

def compute_features():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT n.nav_id, n.fund_id, n.price_date, n.nav_value,
               n.daily_return, n.benchmark_return, n.market_regime,
               f.cssf_threshold, f.fund_type
        FROM fact_nav_pricing n
        JOIN dim_fund f ON n.fund_id = f.fund_id
        ORDER BY n.fund_id, n.price_date
    """, engine)

    enriched = []
    for fund_id, grp in df.groupby("fund_id"):
        g = grp.sort_values("price_date").copy()

        # Rolling 20-day volatility — how much the NAV is moving day to day
        g["rolling_volatility"] = g["daily_return"].rolling(20).std()

        # Tracking error — how much the fund deviates from its benchmark
        active = g["daily_return"] - g["benchmark_return"]
        g["tracking_error"] = active.rolling(20).std()

        # Drawdown — how far NAV has fallen from its recent 60-day peak
        roll_max = g["nav_value"].rolling(60, min_periods=1).max()
        g["drawdown"] = (g["nav_value"] - roll_max) / roll_max

        # 7-day average return — context window we pass to the LLM later
        g["return_7d_avg"] = g["daily_return"].rolling(7).mean()

        enriched.append(g)

    df_full = pd.concat(enriched, ignore_index=True).dropna(subset=["rolling_volatility"])

    with engine.connect() as conn:
        for _, row in df_full.iterrows():
            conn.execute(text("""
                UPDATE fact_nav_pricing
                SET rolling_volatility = :rv,
                    tracking_error     = :te,
                    drawdown           = :dd,
                    return_7d_avg      = :r7
                WHERE nav_id = :nid
            """), {
                "rv":  None if pd.isna(row.rolling_volatility) else float(row.rolling_volatility),
                "te":  None if pd.isna(row.tracking_error)     else float(row.tracking_error),
                "dd":  None if pd.isna(row.drawdown)           else float(row.drawdown),
                "r7":  None if pd.isna(row.return_7d_avg)      else float(row.return_7d_avg),
                "nid": int(row.nav_id),
            })
        conn.commit()

    log.info(f"Features computed and written for {len(df_full)} rows.")

if __name__ == "__main__":
    compute_features()