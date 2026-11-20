# Fund NAV Anomaly Detection

Which daily fund prices need a second look? This project simulates daily net asset values (NAVs) for ten funds, records unusual movements and statistical alerts in PostgreSQL, and ranks observations for analyst review.

| Stage | Observations |
| --- | ---: |
| Simulated daily fund prices across 10 funds | 7,830 |
| Eligible after the rolling-history requirement | 7,640 |
| Flagged by at least one method | 2,029 |
| Crossed a configured daily-return threshold | 1,840 |
| Threshold alerts meeting the priority rule | 95 |

The detector measures daily price changes. A large change can reflect a genuine market move or a bad input. CSSF Circular 24/856 §4.2 addresses the size of a NAV calculation error relative to NAV. These simulated alerts support triage and analyst review. [CSSF Circular 24/856](https://www.cssf.lu/wp-content/uploads/CSSF24_856eng.pdf)

## One alert

On 19 August 2024 the simulated LUX Mixed Conservative Fund rose by 2.58%, above its 0.50% daily-return threshold. It received a priority score of 81.36. An analyst can compare that move with the benchmark and check the pricing inputs.

## How the pipeline works

1. Generate fund prices and inject scenarios such as stale prices and large price shifts.
2. Calculate rolling volatility, tracking error and drawdown.
3. Combine statistical signals from z-scores, Isolation Forest and Local Outlier Factor with a configured return threshold.
4. Write alert details and scores to PostgreSQL. The priority rule selects threshold alerts with a score of at least 60.
5. Optionally generate review notes for priority cases. Power BI can visualize the results.

## Run detection locally

Use Python 3.12 and an empty UTF-8 PostgreSQL database dedicated to this demo. From the repository root, create and activate a virtual environment, install the pinned packages, then copy `.env.example` to `.env` and fill in the local database settings. Keep your local credentials in `.env`.

```bash
python -m venv nav_env
# macOS/Linux: source nav_env/bin/activate
# PowerShell: .\nav_env\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pipeline.run_pipeline --detect-only
python -m pytest -q -k "not rationales"
```

Supply `GROQ_API_KEY` and omit `--detect-only` to request optional rationale generation. Treat generated explanations as suggestions for review.

The [older Power BI workbook](dashboard/nav_dashboard.pbix) and [slide deck](NAV-Anomaly-Intelligence-Platform.pdf) use “breach” for simulated daily-return alerts. Use the definitions and counts above when reading those assets.
