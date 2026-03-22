import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text
from database.db_connection import get_engine
from utils.logger import get_logger

log = get_logger("anomaly_detection")

Z_THRESHOLD       = 3.0
IF_CONTAMINATION  = 0.03
LOF_CONTAMINATION = 0.03
LOF_N_NEIGHBORS   = 20

def z_score(series):
    std = series.std()
    return (series - series.mean()) / std if std != 0 else pd.Series(0.0, index=series.index)

def run_anomaly_detection():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT n.nav_id, n.fund_id, n.price_date, n.nav_value,
               n.daily_return, n.rolling_volatility, n.tracking_error,
               n.drawdown, n.return_7d_avg, n.benchmark_return, n.market_regime,
               f.cssf_threshold, f.fund_type, f.fund_name, f.asset_class
        FROM fact_nav_pricing n
        JOIN dim_fund f ON n.fund_id = f.fund_id
        WHERE n.rolling_volatility IS NOT NULL
        ORDER BY n.fund_id, n.price_date
    """, engine)

    all_flags = []

    for fund_id, grp in df.groupby("fund_id"):
        g = grp.sort_values("price_date").copy()
        cssf_thr  = float(g["cssf_threshold"].iloc[0])
        fund_name = g["fund_name"].iloc[0]

        # LAYER 1A: Z-score
        # Measures how many standard deviations a return is from the fund's average
        # Anything beyond 3 standard deviations is flagged
        g["z_score"] = z_score(g["daily_return"])

        # LAYER 1B: Isolation Forest
        # Builds random decision trees and isolates anomalies — they get isolated faster
        # because they're few and different. Standard in financial fraud detection.
        feat_cols = ["daily_return", "rolling_volatility", "tracking_error", "drawdown"]
        X  = g[feat_cols].fillna(0).values
        Xs = StandardScaler().fit_transform(X)

        iso = IsolationForest(contamination=IF_CONTAMINATION, n_estimators=100, random_state=42)
        iso.fit(Xs)
        raw_if = iso.score_samples(Xs)
        min_if, max_if = raw_if.min(), raw_if.max()
        g["isolation_score"] = 1 - (raw_if - min_if) / (max_if - min_if + 1e-9)
        g["iso_flag"]        = iso.predict(Xs) == -1

        # LAYER 1C: Local Outlier Factor
        # Compares each point's density to its neighbours — anomalies are in sparse regions
        # Using two models gives us cross-validation — if both agree, confidence is higher
        lof = LocalOutlierFactor(n_neighbors=LOF_N_NEIGHBORS, contamination=LOF_CONTAMINATION)
        lof_preds = lof.fit_predict(Xs)
        raw_lof   = -lof.negative_outlier_factor_
        min_lof, max_lof = raw_lof.min(), raw_lof.max()
        g["lof_score"] = (raw_lof - min_lof) / (max_lof - min_lof + 1e-9)
        g["lof_flag"]  = lof_preds == -1

        # LAYER 2: CSSF Regulatory Breach Check
        # This is the differentiator — checks against actual Luxembourg law
        g["abs_return"]       = g["daily_return"].abs()
        g["is_cssf_breach"]   = g["abs_return"] > cssf_thr
        g["breach_magnitude"] = (g["abs_return"] - cssf_thr).clip(lower=0)

        # Combined statistical flag
        g["is_statistical_flag"] = (
            (g["z_score"].abs() > Z_THRESHOLD) | g["iso_flag"] | g["lof_flag"]
        )

        # Composite risk score 0-100
        z_norm    = (g["z_score"].abs() / Z_THRESHOLD).clip(upper=1)
        cssf_norm = (g["abs_return"] / (cssf_thr * 5)).clip(upper=1)
        g["risk_score"] = (
            0.30 * g["isolation_score"] * 100 +
            0.20 * g["lof_score"]       * 100 +
            0.25 * z_norm               * 100 +
            0.25 * cssf_norm            * 100
        ).round(2)

        flagged = g[g["is_statistical_flag"] | g["is_cssf_breach"]].copy()
        log.info(f"{fund_name}: {len(flagged)} flagged rows")
        all_flags.append(flagged)

    df_flagged = pd.concat(all_flags, ignore_index=True)

    with engine.connect() as conn:
        for _, row in df_flagged.iterrows():
            conn.execute(text("""
                INSERT INTO fact_anomalies (
                    nav_id, fund_id, price_date, nav_value, daily_return,
                    z_score, isolation_score, lof_score, risk_score,
                    is_statistical_flag, is_cssf_breach,
                    cssf_threshold, breach_magnitude, market_regime
                ) VALUES (
                    :nav_id, :fund_id, :price_date, :nav_value, :daily_return,
                    :z_score, :isolation_score, :lof_score, :risk_score,
                    :is_stat, :is_cssf, :cssf_thr, :breach, :regime
                ) ON CONFLICT DO NOTHING
            """), {
                "nav_id":          int(row.nav_id),
                "fund_id":         int(row.fund_id),
                "price_date":      row.price_date,
                "nav_value":       float(row.nav_value),
                "daily_return":    float(row.daily_return),
                "z_score":         float(row.z_score),
                "isolation_score": float(row.isolation_score),
                "lof_score":       float(row.lof_score),
                "risk_score":      float(row.risk_score),
                "is_stat":         bool(row.is_statistical_flag),
                "is_cssf":         bool(row.is_cssf_breach),
                "cssf_thr":        float(row.cssf_threshold),
                "breach":          float(row.breach_magnitude),
                "regime":          str(row.market_regime),
            })
        conn.commit()

    log.info(f"Total anomalies written to fact_anomalies: {len(df_flagged)}")

if __name__ == "__main__":
    run_anomaly_detection()