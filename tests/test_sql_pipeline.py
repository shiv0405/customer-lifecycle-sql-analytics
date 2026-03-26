from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from customer_lifecycle_sql_analytics.config import ProjectPaths
from customer_lifecycle_sql_analytics.warehouse import run_warehouse_pipeline


def test_sql_pipeline_builds_outputs_and_warehouse(tmp_path: Path) -> None:
    paths = ProjectPaths.from_root(tmp_path)
    result = run_warehouse_pipeline(paths, accounts=120, months=16, seed=11)

    db_path = paths.warehouse_dir / "customer_lifecycle_analytics.db"
    assert db_path.exists()
    assert (paths.processed_dir / "cohort_retention.csv").exists()
    assert (paths.processed_dir / "region_scorecard.csv").exists()
    assert (paths.processed_dir / "segment_performance.csv").exists()
    assert (paths.processed_dir / "renewal_pipeline.csv").exists()
    assert (paths.artifacts_dir / "executive_summary.html").exists()
    assert result["snapshot"]["summary"]["active_accounts"] > 0

    with sqlite3.connect(db_path) as connection:
        cohort = pd.read_sql_query(
            "SELECT retention_pct FROM mart_cohort_retention WHERE months_since_signup = 0 ORDER BY cohort_month LIMIT 5",
            connection,
        )
        risk = pd.read_sql_query(
            "SELECT risk_score FROM mart_at_risk_accounts ORDER BY risk_score DESC LIMIT 5",
            connection,
        )
        renewal = pd.read_sql_query(
            "SELECT accounts_due, intervention_accounts FROM mart_renewal_pipeline ORDER BY accounts_due DESC LIMIT 20",
            connection,
        )

    assert not cohort.empty
    assert cohort["retention_pct"].min() == 1.0
    assert not risk.empty
    assert not renewal.empty
    assert (renewal["intervention_accounts"] <= renewal["accounts_due"]).all()
    assert risk["risk_score"].iloc[0] >= risk["risk_score"].iloc[-1]
