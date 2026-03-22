# CSSF-Compliant NAV Anomaly Intelligence Platform

An end-to-end AI pipeline that detects pricing anomalies in Luxembourg 
investment fund NAV data, cross-references them against CSSF Circular 
24/856 regulatory materiality thresholds, and uses a generative AI model 
to produce structured audit rationales — visualized in a Power BI dashboard.

## Regulatory Framework

CSSF Circular 24/856 — Luxembourg NAV error materiality thresholds:

| Fund Type     | Threshold |
|---------------|-----------|
| Money Market  | 0.20%     |
| Bond / Mixed  | 0.50%     |
| Equity        | 1.00%     |

## Architecture
```
Synthetic NAV Data (Python + numpy)
         ↓
Causal Anomaly Injection (fat_finger, stale_price, fx_shock,
                          liquidity_event, corporate_action)
         ↓
Feature Engineering (volatility, tracking_error, drawdown)
         ↓
Dual-Layer Detection:
  Layer 1: Isolation Forest + LOF + Z-score (statistical)
  Layer 2: CSSF 24/856 Rule Engine (regulatory)
         ↓
PostgreSQL Database
         ↓
Groq LLM → Structured Audit Rationales
         ↓
Power BI Dashboard (4 pages)
```

## Key Results

- 7,640 daily NAV prices processed across 10 Luxembourg funds (2023-2025)
- 2,029 anomalies detected across 3 asset classes
- 1,840 CSSF regulatory threshold breaches identified
- 95 highest-priority entries escalated for AI-generated audit rationales
- Average risk score of escalated entries: 75.16/100

## Tech Stack

- Python 3.14, pandas, numpy, scikit-learn
- PostgreSQL 18, SQLAlchemy
- Groq API (meta-llama/llama-4-scout-17b-16e-instruct)
- Power BI Desktop
- pytest (6/6 tests passing)

## How to Run
```bash
git clone https://github.com/yourusername/nav-anomaly-platform
cd nav-anomaly-platform
python -m venv nav_env
nav_env\Scripts\activate
pip install -r requirements.txt
cp .env.example .env  # add your credentials
python -m pipeline.run_pipeline
```

## Project Context

Built as a portfolio project targeting Deloitte Luxembourg's 
AI & Data Trainee internship. Demonstrates data engineering, 
ML anomaly detection, GenAI integration, Luxembourg regulatory 
domain knowledge, and executive-ready visualization.
```

Save with **Ctrl+S**.

---

## Step 3 — Create .env.example

Root folder → New File → `.env.example` → paste:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nav_platform
DB_USER=postgres
DB_PASSWORD=your_password_here
GROQ_API_KEY=your_groq_api_key_here