import pytest
from sqlalchemy import text
from database.db_connection import get_engine

@pytest.fixture
def engine():
    return get_engine()

def count(engine, query):
    with engine.connect() as conn:
        return conn.execute(text(query)).scalar()

def test_funds_loaded(engine):
    assert count(engine, "SELECT COUNT(*) FROM dim_fund") == 10

def test_nav_rows_sufficient(engine):
    assert count(engine, "SELECT COUNT(*) FROM fact_nav_pricing") >= 7000

def test_no_negative_nav(engine):
    assert count(engine,
        "SELECT COUNT(*) FROM fact_nav_pricing WHERE nav_value < 0") == 0

def test_anomalies_detected(engine):
    assert count(engine, "SELECT COUNT(*) FROM fact_anomalies") > 0

def test_cssf_breaches_exist(engine):
    assert count(engine,
        "SELECT COUNT(*) FROM fact_anomalies WHERE is_cssf_breach = TRUE") > 0

def test_rationales_generated(engine):
    assert count(engine,
        "SELECT COUNT(*) FROM fact_anomalies WHERE llm_rationale IS NOT NULL") > 0