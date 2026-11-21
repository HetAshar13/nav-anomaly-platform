import os
import time
import json
import pandas as pd
from groq import Groq
from sqlalchemy import text
from dotenv import load_dotenv
from database.db_connection import get_engine
from utils.logger import get_logger

load_dotenv()
log = get_logger("generate_rationale")
MODEL = "meta-llama/llama-4-scout-17b-16e-instruct"

# Select the priority queue of simulated daily-return alerts.
FILTER_QUERY = """
    SELECT a.anomaly_id, a.fund_id, a.nav_id, a.price_date,
           a.nav_value, a.daily_return, a.z_score,
           a.isolation_score, a.lof_score, a.risk_score,
           a.is_cssf_breach, a.cssf_threshold, a.breach_magnitude,
           a.market_regime, a.llm_rationale,
           f.fund_name, f.fund_type
    FROM fact_anomalies a
    JOIN dim_fund f ON a.fund_id = f.fund_id
    WHERE a.llm_rationale IS NULL
      AND a.is_cssf_breach = TRUE
      AND a.risk_score >= 60
    ORDER BY a.risk_score DESC
"""

SYSTEM_PROMPT = """Draft analyst review notes for a simulated daily fund return alert.
Return a JSON object with these three string fields:
- observation: State the return and date using the supplied data.
- likely_cause: Describe a plausible explanation only if the data supports it. Otherwise say the cause is unclear.
- recommended_action: Suggest one check an analyst can perform.
Use one sentence per field. A threshold alert is a prompt for review."""


def build_prompt(row, ctx):
    return (
        f"Fund: {row['fund_name']} ({row['fund_type']})\n"
        f"Date: {row['price_date']}\n"
        f"NAV: {row['nav_value']:.4f} EUR\n"
        f"Daily Return: {row['daily_return']*100:.4f}%\n"
        f"Z-Score: {row['z_score']:.2f}\n"
        f"Risk Score: {row['risk_score']:.1f}/100\n"
        f"Market Regime: {row['market_regime']}\n"
        f"Daily-return threshold: {row['cssf_threshold']*100:.2f}%\n"
        f"Points over threshold: {row['breach_magnitude']*100:.4f}%\n"
        f"7d Avg Return: {ctx['avg_ret']*100:.4f}%\n"
        f"7d Benchmark: {ctx['avg_bench']*100:.4f}%\n\n"
        f"JSON only:"
    )


def get_7d_context(engine, fund_id, price_date):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT daily_return, benchmark_return FROM fact_nav_pricing
            WHERE fund_id = :f AND price_date <= :d
            ORDER BY price_date DESC LIMIT 7
        """), {"f": fund_id, "d": price_date}).fetchall()
    if not rows:
        return {"avg_ret": 0.0, "avg_bench": 0.0, "std_ret": 0.0}
    rets   = [r[0] for r in rows if r[0] is not None]
    bmarks = [r[1] for r in rows if r[1] is not None]
    return {
        "avg_ret":   float(pd.Series(rets).mean()),
        "avg_bench": float(pd.Series(bmarks).mean()),
        "std_ret":   float(pd.Series(rets).std()) if len(rets) > 1 else 0.0,
    }


def safe_log(msg):
    """Strips non-ASCII to avoid Windows cp1252 encoding errors."""
    try:
        log.info(msg.encode("ascii", errors="replace").decode("ascii"))
    except Exception:
        log.info("(log encoding error - entry processed successfully)")


def generate_all_rationales():
    engine = get_engine()
    df = pd.read_sql(FILTER_QUERY, engine)
    total = len(df)

    safe_log(f"Generating rationales for {total} anomalies.")
    safe_log(f"Model: {MODEL}")
    safe_log(f"Estimated time: ~{round(total * 2.5 / 60, 1)} minutes")

    if total == 0:
        safe_log("No rows to process. All entries already have rationales.")
        return

    client = Groq(api_key=os.getenv("GROQ_API_KEY"))
    success, failed = 0, 0

    for _, row in df.iterrows():
        try:
            ctx    = get_7d_context(engine, row["fund_id"], row["price_date"])
            prompt = build_prompt(row, ctx)

            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.1,
                max_tokens=200,
            )

            raw  = resp.choices[0].message.content.strip()
            raw  = raw.replace("```json", "").replace("```", "").strip()
            data = json.loads(raw)
            fields = ("observation", "likely_cause", "recommended_action")
            if not isinstance(data, dict) or any(
                not isinstance(data.get(field), str) or not data[field].strip()
                for field in fields
            ):
                raise ValueError("Expected three nonempty rationale fields")

            rationale = (
                f"OBSERVATION: {data['observation'].strip()} "
                f"CAUSE: {data['likely_cause'].strip()} "
                f"ACTION: {data['recommended_action'].strip()}"
            )

            with engine.connect() as conn:
                conn.execute(text("""
                    UPDATE fact_anomalies
                    SET llm_rationale = :r, llm_confidence = NULL
                    WHERE anomaly_id = :id
                """), {
                    "r":  rationale,
                    "id": int(row["anomaly_id"])
                })
                conn.commit()

            success += 1
            safe_log(
                f"[{success}/{total}] Alert | "
                f"{row['fund_name']} | "
                f"{str(row['price_date'])} | "
                f"Score:{row['risk_score']:.0f}"
            )

            time.sleep(2.5)

        except ValueError:
            failed += 1
            safe_log(f"Invalid rationale for anomaly {row['anomaly_id']}; left pending")

        except Exception as e:
            failed += 1
            err_msg = str(e).encode("ascii", errors="replace").decode("ascii")
            safe_log(f"Error on anomaly {row['anomaly_id']}: {err_msg[:200]}")
            time.sleep(10)

    safe_log(f"Complete. Success: {success} | Failed: {failed}")


if __name__ == "__main__":
    generate_all_rationales()
