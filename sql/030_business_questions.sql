-- Which cohort retains best by month 6?
SELECT
    cohort_month,
    retention_pct
FROM mart_cohort_retention
WHERE months_since_signup = 6
ORDER BY retention_pct DESC;

-- Which regions have the weakest utilization and highest renewal exposure in the latest month?
SELECT
    metric_month,
    region,
    arr_usd,
    avg_utilization_pct,
    renewal_exposure_usd
FROM mart_region_scorecard
WHERE metric_month = (SELECT MAX(metric_month) FROM mart_region_scorecard)
ORDER BY renewal_exposure_usd DESC, avg_utilization_pct ASC;

-- Which channels create activated accounts most efficiently?
SELECT
    cohort_month,
    region,
    acquisition_channel,
    spend_usd,
    new_accounts,
    activated_accounts,
    cac_proxy_usd,
    ninety_day_activation_pct
FROM mart_channel_efficiency
WHERE new_accounts > 0
ORDER BY ninety_day_activation_pct DESC, cac_proxy_usd ASC;

-- Which accounts should be reviewed first?
SELECT
    account_id,
    account_name,
    region,
    arr_usd,
    risk_score,
    renewal_due_in_days
FROM mart_at_risk_accounts
ORDER BY risk_score DESC, arr_usd DESC
LIMIT 25;

-- Which segments carry the highest renewal exposure in the latest month?
SELECT
    metric_month,
    segment,
    arr_usd,
    renewal_exposure_usd,
    avg_utilization_pct,
    net_revenue_retention_proxy
FROM mart_segment_performance
WHERE metric_month = (SELECT MAX(metric_month) FROM mart_segment_performance)
ORDER BY renewal_exposure_usd DESC, avg_utilization_pct ASC;

-- Which renewal groups need the most intervention attention right now?
SELECT
    metric_month,
    region,
    segment,
    renewal_window,
    accounts_due,
    arr_due_usd,
    intervention_accounts
FROM mart_renewal_pipeline
WHERE metric_month = (SELECT MAX(metric_month) FROM mart_renewal_pipeline)
ORDER BY intervention_accounts DESC, arr_due_usd DESC;
