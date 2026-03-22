from sqlalchemy import text
from database.db_connection import get_engine
from utils.logger import get_logger

log = get_logger("validate_pipeline")

def validate():
    engine = get_engine()
    checks = []

    with engine.connect() as conn:
        def count(q):
            return conn.execute(text(q)).scalar()

        checks.append(("dim_fund has 10 funds",
            count("SELECT COUNT(*) FROM dim_fund") == 10))

        checks.append(("fact_nav_pricing has 7000+ rows",
            count("SELECT COUNT(*) FROM fact_nav_pricing") >= 7000))

        checks.append(("No negative NAV values",
            count("SELECT COUNT(*) FROM fact_nav_pricing WHERE nav_value < 0") == 0))

        checks.append(("Injected anomalies exist",
            count("SELECT COUNT(*) FROM fact_nav_pricing WHERE is_injected_anomaly = TRUE") > 0))

        checks.append(("Features computed",
            count("SELECT COUNT(*) FROM fact_nav_pricing WHERE rolling_volatility IS NOT NULL") >= 6000))

        checks.append(("Anomalies detected",
            count("SELECT COUNT(*) FROM fact_anomalies") > 0))

        checks.append(("CSSF breaches exist",
            count("SELECT COUNT(*) FROM fact_anomalies WHERE is_cssf_breach = TRUE") > 0))

        checks.append(("LLM rationales generated",
            count("SELECT COUNT(*) FROM fact_anomalies WHERE llm_rationale IS NOT NULL") > 0))

    print("\n" + "="*55)
    print("PIPELINE VALIDATION REPORT")
    print("="*55)
    all_pass = True
    for label, passed in checks:
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}]  {label}")
        if not passed:
            all_pass = False
    print("="*55)
    print("RESULT:", "ALL CHECKS PASSED" if all_pass else "SOME CHECKS FAILED")
    print("="*55 + "\n")
    return all_pass

if __name__ == "__main__":
    validate()