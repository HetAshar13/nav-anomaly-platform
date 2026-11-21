import argparse
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.run_schema import run_schema
from data_generation.generate_funds import load_funds
from data_generation.generate_nav_data import generate_all_nav_data
from data_generation.inject_anomalies import inject_anomalies
from feature_engineering.compute_features import compute_features
from ml_models.anomaly_detection import run_anomaly_detection
from utils.validate_pipeline import validate
from utils.logger import get_logger

log = get_logger("run_pipeline")

STEPS = [
    ("Schema creation",          run_schema),
    ("Fund master data",         load_funds),
    ("NAV data generation",      generate_all_nav_data),
    ("Anomaly injection",        inject_anomalies),
    ("Feature engineering",      compute_features),
    ("Anomaly detection",        run_anomaly_detection),
]

def run(*, detect_only=False):
    steps = STEPS.copy()
    if not detect_only:
        from llm.generate_rationale import generate_all_rationales

        steps.append(("LLM rationale generation", generate_all_rationales))

    log.info("="*60)
    log.info("FUND NAV ANOMALY DETECTION — PIPELINE START")
    log.info("="*60)

    for name, fn in steps:
        log.info(f">>> STEP: {name}")
        try:
            fn()
            log.info(f"    DONE: {name}")
        except Exception as e:
            log.error(f"    FAILED: {name} — {e}")
            log.error("    Fix the error and re-run. Use utils/reset_db.py to start fresh.")
            sys.exit(1)

    log.info("Running validation...")
    if not validate(include_rationales=not detect_only):
        sys.exit(1)
    log.info("Pipeline complete. Open Power BI to view results.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the simulated fund NAV pipeline.")
    parser.add_argument("--detect-only", action="store_true", help="Skip external rationale generation.")
    run(detect_only=parser.parse_args().detect_only)
