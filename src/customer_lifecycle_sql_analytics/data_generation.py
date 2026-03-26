from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from random import Random

import pandas as pd


REGIONS = ["North America", "Europe", "APAC", "LATAM", "Middle East", "Africa"]
SEGMENTS = ["enterprise", "mid-market", "commercial"]
CHANNELS = ["paid-search", "content", "partner", "events", "outbound"]


@dataclass(frozen=True)
class GeneratedLifecycleData:
    accounts: pd.DataFrame
    monthly_metrics: pd.DataFrame
    invoices: pd.DataFrame
    marketing_spend: pd.DataFrame


def month_starts(months: int, start: date = date(2024, 1, 1)) -> list[date]:
    values: list[date] = []
    year = start.year
    month = start.month
    for _ in range(months):
        values.append(date(year, month, 1))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return values


def generate_lifecycle_data(accounts: int = 420, months: int = 24, seed: int = 19) -> GeneratedLifecycleData:
    rng = Random(seed)
    months_list = month_starts(months)
    account_rows = _generate_accounts(accounts, months_list, rng)
    monthly_rows, invoice_rows = _generate_account_metrics(account_rows, months_list, rng)
    marketing_rows = _generate_marketing_spend(months_list, rng)
    return GeneratedLifecycleData(
        accounts=pd.DataFrame(account_rows),
        monthly_metrics=pd.DataFrame(monthly_rows),
        invoices=pd.DataFrame(invoice_rows),
        marketing_spend=pd.DataFrame(marketing_rows),
    )


def _generate_accounts(account_count: int, months_list: list[date], rng: Random) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    signup_window = months_list[:-4]
    for index in range(account_count):
        region = REGIONS[index % len(REGIONS)]
        segment = SEGMENTS[index % len(SEGMENTS)]
        channel = CHANNELS[(index + rng.randint(0, len(CHANNELS) - 1)) % len(CHANNELS)]
        signup = signup_window[rng.randint(0, len(signup_window) - 1)]
        initial_arr = {
            "enterprise": rng.randint(180_000, 820_000),
            "mid-market": rng.randint(60_000, 240_000),
            "commercial": rng.randint(18_000, 80_000),
        }[segment]
        rows.append(
            {
                "account_id": f"ACC-{1000 + index}",
                "account_name": f"{region[:2].upper()}-Customer-{index + 1:03d}",
                "region": region,
                "segment": segment,
                "acquisition_channel": channel,
                "signup_date": signup.isoformat(),
                "initial_arr_usd": float(initial_arr),
            }
        )
    return rows


def _generate_account_metrics(
    accounts: list[dict[str, object]],
    months_list: list[date],
    rng: Random,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    monthly_rows: list[dict[str, object]] = []
    invoice_rows: list[dict[str, object]] = []

    for account in accounts:
        signup = date.fromisoformat(str(account["signup_date"]))
        segment = str(account["segment"])
        current_arr = float(account["initial_arr_usd"])
        base_seats = {"enterprise": 260, "mid-market": 95, "commercial": 30}[segment]
        growth_bias = rng.uniform(-0.03, 0.08)
        health_bias = rng.uniform(-0.14, 0.16)
        churn_month: date | None = None

        for month_start in months_list:
            if month_start < signup:
                continue
            months_since_signup = (month_start.year - signup.year) * 12 + (month_start.month - signup.month)
            if churn_month and month_start >= churn_month:
                break

            if months_since_signup > 0:
                movement = rng.uniform(-0.07, 0.06) + growth_bias + health_bias * 0.18
                current_arr = max(current_arr * (1 + movement), 0)

            if months_since_signup > 3:
                churn_probability = 0.008
                if growth_bias + health_bias < -0.05:
                    churn_probability += 0.024
                if growth_bias < -0.01:
                    churn_probability += 0.01
                if rng.random() < churn_probability:
                    churn_month = month_start
                    current_arr = 0

            if months_since_signup > 0 and current_arr == 0:
                churn_month = month_start

            active_flag = 1 if current_arr > 0 else 0
            seats_purchased = max(12, int(base_seats * rng.uniform(0.85, 1.18)))
            utilization_draw = rng.uniform(0.34, 0.94) + health_bias * 0.18
            active_seats = 0 if active_flag == 0 else max(4, int(seats_purchased * max(0.18, min(0.96, utilization_draw))))
            feature_adoption_pct = 0 if active_flag == 0 else round(max(0.12, min(0.96, rng.uniform(0.28, 0.91) + health_bias * 0.12)), 4)
            support_tickets = 0 if active_flag == 0 else int(max(0, rng.uniform(1, 11) + (0.55 - feature_adoption_pct) * 10 - health_bias * 4))
            csat_score = 0 if active_flag == 0 else round(max(51.0, min(95.0, rng.uniform(68, 94) - support_tickets * 0.85 + health_bias * 6)), 2)
            renewal_due_in_days = 365 - ((months_since_signup % 12) * 30)

            monthly_rows.append(
                {
                    "metric_month": month_start.strftime("%Y-%m"),
                    "account_id": account["account_id"],
                    "arr_usd": round(current_arr, 2),
                    "active_flag": active_flag,
                    "seats_purchased": seats_purchased,
                    "active_seats": active_seats,
                    "feature_adoption_pct": feature_adoption_pct,
                    "support_tickets": max(0, support_tickets),
                    "csat_score": csat_score,
                    "renewal_due_in_days": renewal_due_in_days,
                }
            )

            if active_flag == 1:
                days = monthrange(month_start.year, month_start.month)[1]
                invoice_rows.append(
                    {
                        "invoice_id": f"INV-{account['account_id']}-{month_start.strftime('%Y%m')}",
                        "account_id": account["account_id"],
                        "invoice_date": date(month_start.year, month_start.month, min(days, rng.randint(22, 28))).isoformat(),
                        "amount_usd": round(current_arr / 12 * rng.uniform(0.96, 1.04), 2),
                        "discount_pct": round(rng.uniform(0.01, 0.16), 4),
                        "days_to_pay": int(rng.uniform(3, 34)),
                    }
                )

    return monthly_rows, invoice_rows


def _generate_marketing_spend(months_list: list[date], rng: Random) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for month_start in months_list:
        quarter_boost = 1.18 if month_start.month in {3, 6, 9, 12} else 1.0
        for region in REGIONS:
            region_scale = 1.2 if region in {"North America", "Europe"} else 0.78
            for channel in CHANNELS:
                spend = rng.uniform(18_000, 72_000) * quarter_boost * region_scale
                leads = int(spend / rng.uniform(120, 260))
                sqls = int(leads * rng.uniform(0.18, 0.42))
                rows.append(
                    {
                        "spend_month": month_start.strftime("%Y-%m"),
                        "region": region,
                        "channel": channel,
                        "spend_usd": round(spend, 2),
                        "leads_generated": leads,
                        "sql_generated": sqls,
                    }
                )
    return rows
