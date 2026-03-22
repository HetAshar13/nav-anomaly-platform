import numpy as np
import pandas as pd
from sqlalchemy import text
from database.db_connection import get_engine
from utils.logger import get_logger

log = get_logger("generate_nav_data")

ASSET_PARAMS = {
    "Equity": (0.18, 0.07, 0.85),
    "Bond":   (0.05, 0.03, 0.80),
    "Mixed":  (0.10, 0.05, 0.75),
    "MMF":    (0.001,0.035,0.50),
}

def get_regime_schedule(n_days, seed=0):
    np.random.seed(seed)
    REGIME_PARAMS = {
        "bull":    {"drift_mult": 2.0,  "vol_mult": 0.8},
        "neutral": {"drift_mult": 1.0,  "vol_mult": 1.0},
        "bear":    {"drift_mult": -1.5, "vol_mult": 1.5},
        "stress":  {"drift_mult": -2.0, "vol_mult": 3.0},
    }
    regime_order = ["bull", "neutral", "bear", "neutral", "stress",
                    "neutral", "bull", "neutral", "bear", "neutral"]
    regimes = []
    day, idx = 0, 0
    while day < n_days:
        regime = regime_order[idx % len(regime_order)]
        duration = min(np.random.randint(40, 120), n_days - day)
        regimes.extend([regime] * duration)
        day += duration
        idx += 1
    return regimes[:n_days], REGIME_PARAMS

def generate_price_series(asset_class, initial_nav, n_days, regimes, regime_params, seed=0):
    np.random.seed(seed)
    annual_vol, annual_drift, bench_corr = ASSET_PARAMS[asset_class]
    daily_vol   = annual_vol   / np.sqrt(252)
    daily_drift = annual_drift / 252

    nav_values, benchmark_returns, fund_returns = [initial_nav], [], []

    for i in range(n_days):
        rp  = regime_params[regimes[i]]
        d_v = daily_vol   * rp["vol_mult"]
        d_d = daily_drift * rp["drift_mult"]
        b_ret = np.random.normal(d_d * 0.8, d_v * 0.9)
        f_ret = bench_corr * b_ret + np.random.normal(0, d_v * np.sqrt(1 - bench_corr**2)) + d_d
        nav_values.append(nav_values[-1] * (1 + f_ret))
        benchmark_returns.append(round(b_ret, 8))
        fund_returns.append(round(f_ret, 8))

    return np.array(nav_values[1:]), benchmark_returns, fund_returns

def get_business_days(start="2023-01-02", end="2025-12-31"):
    return [d.date() for d in pd.date_range(start=start, end=end, freq="B")]

def generate_all_nav_data():
    engine  = get_engine()
    dates   = get_business_days()
    n_days  = len(dates)
    regimes, regime_params = get_regime_schedule(n_days, seed=7)

    with engine.connect() as conn:
        funds = conn.execute(text(
            "SELECT fund_id, fund_name, asset_class, initial_nav FROM dim_fund ORDER BY fund_id"
        )).fetchall()

    all_records = []
    for fund_id, fund_name, asset_class, initial_nav in funds:
        log.info(f"Generating NAV series: {fund_name}")
        nav_vals, bench_rets, daily_rets = generate_price_series(
            asset_class, initial_nav, n_days, regimes, regime_params, seed=fund_id * 42
        )
        for i, d in enumerate(dates):
            all_records.append({
                "fund_id":             fund_id,
                "price_date":          d,
                "total_net_assets":    round(float(nav_vals[i]) * 1_000_000, 2),
                "shares_outstanding":  1_000_000,
                "nav_value":           round(float(nav_vals[i]), 6),
                "benchmark_return":    bench_rets[i],
                "daily_return":        daily_rets[i],
                "market_regime":       regimes[i],
                "is_injected_anomaly": False,
                "anomaly_type":        None,
            })

    df = pd.DataFrame(all_records)
    df.to_sql("fact_nav_pricing", engine, if_exists="append", index=False, chunksize=500)
    log.info(f"Loaded {len(df)} NAV records into fact_nav_pricing.")

if __name__ == "__main__":
    generate_all_nav_data()