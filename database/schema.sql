CREATE TABLE IF NOT EXISTS dim_fund (
    fund_id          SERIAL PRIMARY KEY,
    fund_name        VARCHAR(100) NOT NULL,
    fund_type        VARCHAR(50)  NOT NULL,
    asset_class      VARCHAR(50)  NOT NULL,
    base_currency    VARCHAR(10)  DEFAULT 'EUR',
    benchmark        VARCHAR(100),
    cssf_threshold   FLOAT        NOT NULL,
    initial_nav      FLOAT        NOT NULL,
    created_at       TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_nav_pricing (
    nav_id               SERIAL PRIMARY KEY,
    fund_id              INT REFERENCES dim_fund(fund_id),
    price_date           DATE    NOT NULL,
    total_net_assets     FLOAT,
    shares_outstanding   FLOAT,
    nav_value            FLOAT   NOT NULL,
    benchmark_return     FLOAT,
    daily_return         FLOAT,
    rolling_volatility   FLOAT,
    tracking_error       FLOAT,
    drawdown             FLOAT,
    return_7d_avg        FLOAT,
    market_regime        VARCHAR(20),
    is_injected_anomaly  BOOLEAN DEFAULT FALSE,
    anomaly_type         VARCHAR(50),
    created_at           TIMESTAMP DEFAULT NOW(),
    UNIQUE(fund_id, price_date)
);

CREATE TABLE IF NOT EXISTS fact_anomalies (
    anomaly_id           SERIAL PRIMARY KEY,
    nav_id               INT REFERENCES fact_nav_pricing(nav_id),
    fund_id              INT REFERENCES dim_fund(fund_id),
    price_date           DATE    NOT NULL,
    nav_value            FLOAT,
    daily_return         FLOAT,
    z_score              FLOAT,
    isolation_score      FLOAT,
    lof_score            FLOAT,
    risk_score           FLOAT,
    is_statistical_flag  BOOLEAN DEFAULT FALSE,
    is_cssf_breach       BOOLEAN DEFAULT FALSE,
    cssf_threshold       FLOAT,
    breach_magnitude     FLOAT,
    market_regime        VARCHAR(20),
    anomaly_cause        VARCHAR(50),
    llm_rationale        TEXT,
    llm_confidence       VARCHAR(20),
    reviewed             BOOLEAN DEFAULT FALSE,
    created_at           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nav_fund_date ON fact_nav_pricing(fund_id, price_date);
CREATE INDEX IF NOT EXISTS idx_anomaly_risk   ON fact_anomalies(risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_cssf   ON fact_anomalies(is_cssf_breach);
CREATE INDEX IF NOT EXISTS idx_anomaly_date   ON fact_anomalies(price_date);