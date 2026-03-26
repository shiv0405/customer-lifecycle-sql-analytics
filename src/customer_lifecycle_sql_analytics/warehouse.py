from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd

from .config import ProjectPaths
from .data_generation import GeneratedLifecycleData, generate_lifecycle_data

SQL_ROOT = Path(__file__).resolve().parents[2] / "sql"


def run_warehouse_pipeline(
    paths: ProjectPaths,
    accounts: int = 420,
    months: int = 24,
    seed: int = 19,
) -> dict[str, object]:
    paths.ensure()
    generated = generate_lifecycle_data(accounts=accounts, months=months, seed=seed)
    _write_raw_csvs(paths, generated)
    db_path = paths.warehouse_dir / "customer_lifecycle_analytics.db"
    if db_path.exists():
        db_path.unlink()

    connection = sqlite3.connect(db_path)
    try:
        _load_to_sqlite(connection, generated)
        _run_sql_file(connection, SQL_ROOT / "001_views.sql")
        _run_sql_file(connection, SQL_ROOT / "010_marts.sql")
        outputs = _export_outputs(connection, paths)
    finally:
        connection.close()

    return outputs


def _write_raw_csvs(paths: ProjectPaths, generated: GeneratedLifecycleData) -> None:
    generated.accounts.to_csv(paths.raw_dir / "accounts.csv", index=False)
    generated.monthly_metrics.to_csv(paths.raw_dir / "monthly_lifecycle_metrics.csv", index=False)
    generated.invoices.to_csv(paths.raw_dir / "invoices.csv", index=False)
    generated.marketing_spend.to_csv(paths.raw_dir / "marketing_spend.csv", index=False)


def _load_to_sqlite(connection: sqlite3.Connection, generated: GeneratedLifecycleData) -> None:
    generated.accounts.to_sql("raw_accounts", connection, index=False, if_exists="replace")
    generated.monthly_metrics.to_sql("raw_monthly_lifecycle_metrics", connection, index=False, if_exists="replace")
    generated.invoices.to_sql("raw_invoices", connection, index=False, if_exists="replace")
    generated.marketing_spend.to_sql("raw_marketing_spend", connection, index=False, if_exists="replace")


def _run_sql_file(connection: sqlite3.Connection, path: Path) -> None:
    connection.executescript(path.read_text(encoding="utf-8"))


def _export_outputs(connection: sqlite3.Connection, paths: ProjectPaths) -> dict[str, object]:
    outputs = {
        "cohort_retention": _query_table(connection, "SELECT * FROM mart_cohort_retention"),
        "revenue_bridge": _query_table(connection, "SELECT * FROM mart_revenue_bridge"),
        "region_scorecard": _query_table(connection, "SELECT * FROM mart_region_scorecard"),
        "channel_efficiency": _query_table(connection, "SELECT * FROM mart_channel_efficiency"),
        "segment_performance": _query_table(connection, "SELECT * FROM mart_segment_performance"),
        "renewal_pipeline": _query_table(connection, "SELECT * FROM mart_renewal_pipeline"),
        "at_risk_accounts": _query_table(connection, "SELECT * FROM mart_at_risk_accounts"),
    }

    outputs["cohort_retention"].to_csv(paths.processed_dir / "cohort_retention.csv", index=False)
    outputs["revenue_bridge"].to_csv(paths.processed_dir / "revenue_bridge.csv", index=False)
    outputs["region_scorecard"].to_csv(paths.processed_dir / "region_scorecard.csv", index=False)
    outputs["channel_efficiency"].to_csv(paths.processed_dir / "channel_efficiency.csv", index=False)
    outputs["segment_performance"].to_csv(paths.processed_dir / "segment_performance.csv", index=False)
    outputs["renewal_pipeline"].to_csv(paths.processed_dir / "renewal_pipeline.csv", index=False)
    outputs["at_risk_accounts"].to_csv(paths.processed_dir / "at_risk_accounts.csv", index=False)

    snapshot = _build_snapshot(outputs)
    (paths.artifacts_dir / "dashboard_snapshot.json").write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    (paths.artifacts_dir / "analysis_findings.md").write_text(_analysis_findings(outputs), encoding="utf-8")
    (paths.artifacts_dir / "executive_summary.html").write_text(_executive_html(snapshot, outputs), encoding="utf-8")
    return {"tables": outputs, "snapshot": snapshot}


def _query_table(connection: sqlite3.Connection, query: str) -> pd.DataFrame:
    return pd.read_sql_query(query, connection)


def _build_snapshot(outputs: dict[str, pd.DataFrame]) -> dict[str, object]:
    region_scorecard = outputs["region_scorecard"]
    latest_month = region_scorecard["metric_month"].max()
    latest_region = region_scorecard[region_scorecard["metric_month"] == latest_month].sort_values(
        ["renewal_exposure_usd", "avg_utilization_pct"], ascending=[False, True]
    )
    revenue_bridge = outputs["revenue_bridge"]
    latest_bridge = revenue_bridge.iloc[-1]
    cohort = outputs["cohort_retention"]
    cohort_focus = cohort[cohort["months_since_signup"] == 12]
    if cohort_focus.empty:
        cohort_focus = cohort[cohort["months_since_signup"] == 9]
    if cohort_focus.empty:
        cohort_focus = cohort[cohort["months_since_signup"] == 6]
    cohort_focus = cohort_focus.sort_values("retention_pct", ascending=True).head(5)
    at_risk = outputs["at_risk_accounts"].head(10)
    channel = outputs["channel_efficiency"]
    recent_channel = channel[channel["new_accounts"] >= 2]
    if recent_channel.empty:
        recent_channel = channel[channel["new_accounts"] > 0]
    recent_channel = recent_channel.sort_values(
        ["cohort_month", "ninety_day_activation_pct", "new_accounts", "cac_proxy_usd"],
        ascending=[False, False, False, True],
    ).head(8)
    segment_performance = outputs["segment_performance"]
    latest_segment = segment_performance[segment_performance["metric_month"] == latest_month].sort_values(
        ["renewal_exposure_usd", "avg_utilization_pct"], ascending=[False, True]
    )
    renewal_pipeline = outputs["renewal_pipeline"]
    latest_renewals = renewal_pipeline[renewal_pipeline["metric_month"] == latest_month].sort_values(
        ["arr_due_usd", "intervention_accounts"], ascending=[False, False]
    )

    return {
        "latest_month": latest_month,
        "summary": {
            "regions_tracked": int(region_scorecard["region"].nunique()),
            "segments_tracked": int(segment_performance["segment"].nunique()),
            "active_accounts": int(latest_region["active_accounts"].sum()),
            "current_arr_usd": round(float(latest_region["arr_usd"].sum()), 2),
            "renewal_exposure_usd": round(float(latest_region["renewal_exposure_usd"].sum()), 2),
            "latest_new_arr_usd": round(float(latest_bridge["new_arr_usd"]), 2),
            "latest_churn_arr_usd": round(float(latest_bridge["churn_arr_usd"]), 2),
        },
        "region_watchlist": _json_records(latest_region.head(6)),
        "segment_watchlist": _json_records(latest_segment.head(6)),
        "cohort_retention_focus": _json_records(cohort_focus),
        "channel_efficiency": _json_records(recent_channel),
        "renewal_pipeline_focus": _json_records(latest_renewals.head(10)),
        "risk_queue": _json_records(at_risk),
    }


def _json_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    records = frame.to_dict(orient="records")
    cleaned: list[dict[str, object]] = []
    for record in records:
        cleaned.append({str(key): _json_value(value) for key, value in record.items()})
    return cleaned


def _json_value(value: object) -> object:
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return value


def _analysis_findings(outputs: dict[str, pd.DataFrame]) -> str:
    region_scorecard = outputs["region_scorecard"]
    latest_month = region_scorecard["metric_month"].max()
    latest_region = region_scorecard[region_scorecard["metric_month"] == latest_month].sort_values(
        ["renewal_exposure_usd", "avg_utilization_pct"], ascending=[False, True]
    )
    top_region = latest_region.iloc[0]

    channel = outputs["channel_efficiency"]
    top_channel = channel[channel["new_accounts"] >= 2]
    if top_channel.empty:
        top_channel = channel[channel["new_accounts"] > 0]
    top_channel = top_channel.sort_values(
        ["ninety_day_activation_pct", "new_accounts", "cac_proxy_usd"],
        ascending=[False, False, True],
    ).iloc[0]
    cohort = outputs["cohort_retention"]
    cohort_focus = cohort[cohort["months_since_signup"] == 12]
    if cohort_focus.empty:
        cohort_focus = cohort[cohort["months_since_signup"] == 9]
    if cohort_focus.empty:
        cohort_focus = cohort[cohort["months_since_signup"] == 6]
    weakest_cohort = cohort_focus.sort_values("retention_pct", ascending=True).iloc[0]

    at_risk = outputs["at_risk_accounts"].iloc[0]
    revenue_bridge = outputs["revenue_bridge"].iloc[-1]
    segment_performance = outputs["segment_performance"]
    latest_segment = segment_performance[segment_performance["metric_month"] == latest_month].sort_values(
        ["renewal_exposure_usd", "avg_utilization_pct"], ascending=[False, True]
    )
    top_segment = latest_segment.iloc[0]
    renewal_pipeline = outputs["renewal_pipeline"]
    latest_renewal_focus = renewal_pipeline[renewal_pipeline["metric_month"] == latest_month].sort_values(
        ["intervention_accounts", "arr_due_usd"], ascending=[False, False]
    ).iloc[0]

    return "\n".join(
        [
            "# Analysis Findings",
            "",
            f"- In the latest month ({latest_month}), {top_region['region']} carries the highest renewal exposure at ${top_region['renewal_exposure_usd']:,.0f} while also showing lower utilization.",
            f"- The {top_segment['segment']} segment has the largest current renewal exposure at ${top_segment['renewal_exposure_usd']:,.0f}, making it the first segment to review for retention planning.",
            f"- The weakest highlighted cohort is {weakest_cohort['cohort_month']}, retaining {weakest_cohort['retention_pct'] * 100:.1f}% of accounts by month {int(weakest_cohort['months_since_signup'])}.",
            f"- {top_channel['acquisition_channel']} in {top_channel['region']} is the strongest recent acquisition source by activation rate, reaching {top_channel['ninety_day_activation_pct'] * 100:.1f}% ninety-day activation.",
            f"- The largest current account risk signal is {at_risk['account_name']} in {at_risk['region']} with a risk score of {at_risk['risk_score']:.1f}.",
            f"- The renewal planning queue is heaviest in {latest_renewal_focus['region']} / {latest_renewal_focus['segment']} for the {latest_renewal_focus['renewal_window']} window, with {int(latest_renewal_focus['intervention_accounts'])} accounts needing intervention.",
            f"- The latest revenue bridge shows ${revenue_bridge['new_arr_usd']:,.0f} of new ARR, ${revenue_bridge['expansion_arr_usd']:,.0f} of expansion ARR, and ${revenue_bridge['churn_arr_usd']:,.0f} of churn ARR.",
            "",
            "## Recommended Analyst Follow-Ups",
            "",
            "- Break down renewal exposure by segment and customer success owner.",
            "- Compare activation quality across channels over a rolling three-cohort window instead of one month at a time.",
            "- Investigate the weakest 12-month cohorts for shared onboarding, pricing, or adoption patterns.",
            "- Trace the highest-risk accounts back to support ticket themes and feature adoption gaps.",
        ]
    ) + "\n"


def _executive_html(snapshot: dict[str, object], outputs: dict[str, pd.DataFrame]) -> str:
    summary = snapshot["summary"]
    region_rows = snapshot["region_watchlist"]
    segment_rows = snapshot["segment_watchlist"]
    risk_rows = snapshot["risk_queue"]
    region_table = "".join(
        f"<tr><td>{row['region']}</td><td>${row['arr_usd']:,.0f}</td><td>{row['avg_utilization_pct'] * 100:.1f}%</td><td>${row['renewal_exposure_usd']:,.0f}</td></tr>"
        for row in region_rows
    )
    segment_table = "".join(
        f"<tr><td>{row['segment']}</td><td>${row['arr_usd']:,.0f}</td><td>{row['avg_utilization_pct'] * 100:.1f}%</td><td>${row['renewal_exposure_usd']:,.0f}</td><td>{_format_ratio(row['net_revenue_retention_proxy'])}</td></tr>"
        for row in segment_rows
    )
    risk_table = "".join(
        f"<tr><td>{row['account_name']}</td><td>{row['region']}</td><td>${row['arr_usd']:,.0f}</td><td>{row['risk_score']:.1f}</td><td>{row['renewal_due_in_days']}</td></tr>"
        for row in risk_rows[:8]
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Customer Lifecycle Analytics Warehouse</title>
  <style>
    body {{ margin: 0; font-family: "Segoe UI", sans-serif; background: #f5f7fb; color: #1d2733; }}
    .wrap {{ max-width: 1180px; margin: 0 auto; padding: 32px; }}
    .hero {{ background: linear-gradient(135deg, #0f4c75, #3282b8); color: white; padding: 28px; border-radius: 22px; }}
    .grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-top: 20px; }}
    .card {{ background: white; border-radius: 18px; padding: 18px; box-shadow: 0 16px 36px rgba(18, 32, 51, 0.08); }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid #d7e0ea; }}
    h1, h2, h3 {{ margin-top: 0; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>Customer Lifecycle Analytics Warehouse</h1>
      <p>SQL-first view of customer retention, revenue movement, renewal exposure, and acquisition quality.</p>
    </section>
    <section class="grid">
      <div class="card"><h3>Active Accounts</h3><strong>{summary['active_accounts']}</strong></div>
      <div class="card"><h3>Current ARR</h3><strong>${summary['current_arr_usd']:,.0f}</strong></div>
      <div class="card"><h3>Renewal Exposure</h3><strong>${summary['renewal_exposure_usd']:,.0f}</strong></div>
    </section>
    <section class="card" style="margin-top: 24px;">
      <h2>Regional Watchlist</h2>
      <table>
        <thead><tr><th>Region</th><th>ARR</th><th>Utilization</th><th>Renewal Exposure</th></tr></thead>
        <tbody>
          {region_table}
        </tbody>
      </table>
    </section>
    <section class="card" style="margin-top: 24px;">
      <h2>Segment Watchlist</h2>
      <table>
        <thead><tr><th>Segment</th><th>ARR</th><th>Utilization</th><th>Renewal Exposure</th><th>NRR Proxy</th></tr></thead>
        <tbody>
          {segment_table}
        </tbody>
      </table>
    </section>
    <section class="card" style="margin-top: 24px;">
      <h2>Highest Risk Accounts</h2>
      <table>
        <thead><tr><th>Account</th><th>Region</th><th>ARR</th><th>Risk Score</th><th>Renewal Due</th></tr></thead>
        <tbody>
          {risk_table}
        </tbody>
      </table>
    </section>
  </div>
</body>
</html>
"""


def _format_ratio(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{float(value) * 100:.1f}%"
