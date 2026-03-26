DROP TABLE IF EXISTS mart_cohort_retention;
CREATE TABLE mart_cohort_retention AS
WITH base AS (
    SELECT
        account_id,
        substr(signup_date, 1, 7) AS cohort_month,
        metric_month,
        active_flag
    FROM vw_account_monthly_health
),
cohort_size AS (
    SELECT cohort_month, COUNT(DISTINCT account_id) AS cohort_size
    FROM base
    GROUP BY 1
),
retention AS (
    SELECT
        cohort_month,
        metric_month,
        (
            (CAST(substr(metric_month, 1, 4) AS INTEGER) - CAST(substr(cohort_month, 1, 4) AS INTEGER)) * 12
            + (CAST(substr(metric_month, 6, 2) AS INTEGER) - CAST(substr(cohort_month, 6, 2) AS INTEGER))
        ) AS months_since_signup,
        COUNT(DISTINCT CASE WHEN active_flag = 1 THEN account_id END) AS retained_accounts
    FROM base
    GROUP BY 1, 2, 3
)
SELECT
    r.cohort_month,
    r.metric_month,
    r.months_since_signup,
    c.cohort_size,
    r.retained_accounts,
    ROUND(CAST(r.retained_accounts AS REAL) / NULLIF(c.cohort_size, 0), 4) AS retention_pct
FROM retention r
JOIN cohort_size c ON c.cohort_month = r.cohort_month
ORDER BY r.cohort_month, r.months_since_signup;

DROP TABLE IF EXISTS mart_revenue_bridge;
CREATE TABLE mart_revenue_bridge AS
WITH base AS (
    SELECT
        metric_month,
        account_id,
        arr_usd,
        COALESCE(prior_arr_usd, 0) AS prior_arr_usd
    FROM vw_account_monthly_health
),
classified AS (
    SELECT
        metric_month,
        account_id,
        CASE
            WHEN prior_arr_usd = 0 AND arr_usd > 0 THEN arr_usd
            ELSE 0
        END AS new_arr_usd,
        CASE
            WHEN prior_arr_usd > 0 AND arr_usd > prior_arr_usd THEN arr_usd - prior_arr_usd
            ELSE 0
        END AS expansion_arr_usd,
        CASE
            WHEN prior_arr_usd > 0 AND arr_usd < prior_arr_usd AND arr_usd > 0 THEN prior_arr_usd - arr_usd
            ELSE 0
        END AS contraction_arr_usd,
        CASE
            WHEN prior_arr_usd > 0 AND arr_usd = 0 THEN prior_arr_usd
            ELSE 0
        END AS churn_arr_usd,
        arr_usd
    FROM base
)
SELECT
    metric_month,
    ROUND(SUM(new_arr_usd), 2) AS new_arr_usd,
    ROUND(SUM(expansion_arr_usd), 2) AS expansion_arr_usd,
    ROUND(SUM(contraction_arr_usd), 2) AS contraction_arr_usd,
    ROUND(SUM(churn_arr_usd), 2) AS churn_arr_usd,
    ROUND(SUM(arr_usd), 2) AS ending_arr_usd
FROM classified
GROUP BY 1
ORDER BY 1;

DROP TABLE IF EXISTS mart_region_scorecard;
CREATE TABLE mart_region_scorecard AS
WITH monthly AS (
    SELECT
        metric_month,
        region,
        COUNT(DISTINCT CASE WHEN active_flag = 1 THEN account_id END) AS active_accounts,
        ROUND(SUM(arr_usd), 2) AS arr_usd,
        ROUND(AVG(utilization_pct), 4) AS avg_utilization_pct,
        ROUND(AVG(feature_adoption_pct), 4) AS avg_feature_adoption_pct,
        ROUND(AVG(csat_score), 2) AS avg_csat_score,
        ROUND(AVG(support_tickets), 2) AS avg_support_tickets,
        ROUND(SUM(CASE WHEN renewal_due_in_days <= 90 THEN arr_usd ELSE 0 END), 2) AS renewal_exposure_usd
    FROM vw_account_monthly_health
    GROUP BY 1, 2
),
with_lag AS (
    SELECT
        *,
        LAG(arr_usd) OVER (PARTITION BY region ORDER BY metric_month) AS prior_arr_usd
    FROM monthly
)
SELECT
    metric_month,
    region,
    active_accounts,
    arr_usd,
    avg_utilization_pct,
    avg_feature_adoption_pct,
    avg_csat_score,
    avg_support_tickets,
    renewal_exposure_usd,
    ROUND(CASE WHEN prior_arr_usd IS NULL OR prior_arr_usd = 0 THEN NULL ELSE arr_usd / prior_arr_usd END, 4) AS arr_retention_ratio
FROM with_lag
ORDER BY metric_month, region;

DROP TABLE IF EXISTS mart_channel_efficiency;
CREATE TABLE mart_channel_efficiency AS
WITH acquired AS (
    SELECT
        substr(signup_date, 1, 7) AS cohort_month,
        region,
        acquisition_channel,
        COUNT(*) AS new_accounts,
        ROUND(AVG(initial_arr_usd), 2) AS avg_first_year_arr_usd
    FROM raw_accounts
    GROUP BY 1, 2, 3
),
activation AS (
    SELECT
        a.region,
        a.acquisition_channel,
        substr(a.signup_date, 1, 7) AS cohort_month,
        COUNT(DISTINCT CASE WHEN m.active_flag = 1 AND m.utilization_pct >= 0.55 THEN a.account_id END) AS activated_accounts
    FROM raw_accounts a
    JOIN vw_account_monthly_health m ON m.account_id = a.account_id
    WHERE (
        (CAST(substr(m.metric_month, 1, 4) AS INTEGER) - CAST(substr(a.signup_date, 1, 4) AS INTEGER)) * 12
        + (CAST(substr(m.metric_month, 6, 2) AS INTEGER) - CAST(substr(a.signup_date, 6, 2) AS INTEGER))
    ) <= 2
    GROUP BY 1, 2, 3
),
marketing AS (
    SELECT
        spend_month AS cohort_month,
        region,
        channel AS acquisition_channel,
        ROUND(SUM(spend_usd), 2) AS spend_usd,
        SUM(leads_generated) AS leads_generated,
        SUM(sql_generated) AS sql_generated
    FROM raw_marketing_spend
    GROUP BY 1, 2, 3
)
SELECT
    m.cohort_month,
    m.region,
    m.acquisition_channel,
    m.spend_usd,
    m.leads_generated,
    m.sql_generated,
    COALESCE(a.new_accounts, 0) AS new_accounts,
    COALESCE(act.activated_accounts, 0) AS activated_accounts,
    COALESCE(a.avg_first_year_arr_usd, 0) AS avg_first_year_arr_usd,
    ROUND(m.spend_usd / NULLIF(COALESCE(a.new_accounts, 0), 0), 2) AS cac_proxy_usd,
    ROUND(CAST(COALESCE(act.activated_accounts, 0) AS REAL) / NULLIF(COALESCE(a.new_accounts, 0), 0), 4) AS ninety_day_activation_pct
FROM marketing m
LEFT JOIN acquired a
    ON a.cohort_month = m.cohort_month
   AND a.region = m.region
   AND a.acquisition_channel = m.acquisition_channel
LEFT JOIN activation act
    ON act.cohort_month = m.cohort_month
   AND act.region = m.region
   AND act.acquisition_channel = m.acquisition_channel
ORDER BY m.cohort_month, m.region, m.acquisition_channel;

DROP TABLE IF EXISTS mart_segment_performance;
CREATE TABLE mart_segment_performance AS
WITH monthly AS (
    SELECT
        metric_month,
        segment,
        COUNT(DISTINCT CASE WHEN active_flag = 1 THEN account_id END) AS active_accounts,
        ROUND(SUM(arr_usd), 2) AS arr_usd,
        ROUND(AVG(utilization_pct), 4) AS avg_utilization_pct,
        ROUND(AVG(feature_adoption_pct), 4) AS avg_feature_adoption_pct,
        ROUND(AVG(csat_score), 2) AS avg_csat_score,
        ROUND(AVG(support_tickets), 2) AS avg_support_tickets,
        ROUND(SUM(CASE WHEN renewal_due_in_days <= 90 THEN arr_usd ELSE 0 END), 2) AS renewal_exposure_usd
    FROM vw_account_monthly_health
    GROUP BY 1, 2
),
with_lag AS (
    SELECT
        *,
        LAG(arr_usd) OVER (PARTITION BY segment ORDER BY metric_month) AS prior_arr_usd
    FROM monthly
)
SELECT
    metric_month,
    segment,
    active_accounts,
    arr_usd,
    avg_utilization_pct,
    avg_feature_adoption_pct,
    avg_csat_score,
    avg_support_tickets,
    renewal_exposure_usd,
    ROUND(
        CASE
            WHEN prior_arr_usd IS NULL OR prior_arr_usd = 0 THEN NULL
            ELSE arr_usd / prior_arr_usd
        END,
        4
    ) AS net_revenue_retention_proxy
FROM with_lag
ORDER BY metric_month, segment;

DROP TABLE IF EXISTS mart_renewal_pipeline;
CREATE TABLE mart_renewal_pipeline AS
WITH base AS (
    SELECT
        metric_month,
        region,
        segment,
        account_id,
        account_name,
        arr_usd,
        utilization_pct,
        feature_adoption_pct,
        csat_score,
        support_tickets,
        renewal_due_in_days,
        CASE
            WHEN renewal_due_in_days <= 30 THEN '0-30 days'
            WHEN renewal_due_in_days <= 60 THEN '31-60 days'
            WHEN renewal_due_in_days <= 90 THEN '61-90 days'
            WHEN renewal_due_in_days <= 180 THEN '91-180 days'
            ELSE '181+ days'
        END AS renewal_window,
        CASE
            WHEN utilization_pct < 0.45 OR feature_adoption_pct < 0.4 OR csat_score < 72 OR support_tickets >= 8 THEN 1
            ELSE 0
        END AS intervention_flag
    FROM vw_account_monthly_health
    WHERE active_flag = 1
)
SELECT
    metric_month,
    region,
    segment,
    renewal_window,
    COUNT(DISTINCT account_id) AS accounts_due,
    ROUND(SUM(arr_usd), 2) AS arr_due_usd,
    ROUND(AVG(utilization_pct), 4) AS avg_utilization_pct,
    ROUND(AVG(feature_adoption_pct), 4) AS avg_feature_adoption_pct,
    ROUND(AVG(csat_score), 2) AS avg_csat_score,
    ROUND(AVG(support_tickets), 2) AS avg_support_tickets,
    SUM(intervention_flag) AS intervention_accounts
FROM base
GROUP BY 1, 2, 3, 4
ORDER BY metric_month, region, segment, renewal_window;

DROP TABLE IF EXISTS mart_at_risk_accounts;
CREATE TABLE mart_at_risk_accounts AS
WITH latest_month AS (
    SELECT MAX(metric_month) AS metric_month
    FROM vw_account_monthly_health
)
SELECT
    v.metric_month,
    v.account_id,
    v.account_name,
    v.region,
    v.segment,
    v.acquisition_channel,
    ROUND(v.arr_usd, 2) AS arr_usd,
    ROUND(v.utilization_pct, 4) AS utilization_pct,
    ROUND(v.feature_adoption_pct, 4) AS feature_adoption_pct,
    v.support_tickets,
    ROUND(v.csat_score, 2) AS csat_score,
    v.renewal_due_in_days,
    ROUND(
        (CASE WHEN COALESCE(v.prior_active_seats, 0) > v.active_seats THEN (v.prior_active_seats - v.active_seats) * 0.45 ELSE 0 END)
        + (CASE WHEN v.support_tickets >= 8 THEN v.support_tickets * 1.8 ELSE v.support_tickets * 0.9 END)
        + (CASE WHEN v.csat_score < 72 THEN (72 - v.csat_score) * 1.2 ELSE 0 END)
        + (CASE WHEN v.renewal_due_in_days <= 90 THEN 18 ELSE 0 END)
        + (CASE WHEN v.utilization_pct < 0.45 THEN 14 ELSE 0 END)
    , 1) AS risk_score
FROM vw_account_monthly_health v
JOIN latest_month lm ON lm.metric_month = v.metric_month
WHERE v.active_flag = 1
ORDER BY risk_score DESC, arr_usd DESC;
