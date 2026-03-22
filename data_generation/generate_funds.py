import pandas as pd
from database.db_connection import get_engine
from utils.logger import get_logger

log = get_logger("generate_funds")

FUNDS = [
    ("LUX Equity Alpha Fund",       "Equity", "Equity", "EURO STOXX 50",      0.0100, 150.00),
    ("LUX Equity Growth Fund",      "Equity", "Equity", "MSCI Europe",        0.0100, 220.00),
    ("LUX Equity Tech Fund",        "Equity", "Equity", "MSCI World IT",      0.0100,  98.50),
    ("LUX Bond Investment Fund",    "Bond",   "Bond",   "Bloomberg Euro Agg", 0.0050, 105.20),
    ("LUX Bond Corporate Fund",     "Bond",   "Bond",   "iBoxx EUR Corp",     0.0050, 112.75),
    ("LUX Mixed Balanced Fund",     "Mixed",  "Mixed",  "60/40 Blend",        0.0050, 130.00),
    ("LUX Mixed Conservative Fund", "Mixed",  "Mixed",  "40/60 Blend",        0.0050, 118.40),
    ("LUX Money Market Prime",      "MMF",    "MMF",    "€STR",               0.0020, 100.01),
    ("LUX Money Market Gov",        "MMF",    "MMF",    "€STR",               0.0020, 100.00),
    ("LUX Money Market Corp",       "MMF",    "MMF",    "€STR",               0.0020, 100.02),
]

def load_funds():
    engine = get_engine()
    records = [
        {"fund_name": n, "fund_type": t, "asset_class": a, "base_currency": "EUR",
         "benchmark": b, "cssf_threshold": thr, "initial_nav": nav}
        for (n, t, a, b, thr, nav) in FUNDS
    ]
    df = pd.DataFrame(records)
    df.to_sql("dim_fund", engine, if_exists="append", index=False)
    log.info(f"Loaded {len(df)} funds into dim_fund.")

if __name__ == "__main__":
    load_funds()