DROP VIEW IF EXISTS vw_account_monthly_health;

CREATE VIEW vw_account_monthly_health AS
WITH invoice_monthly AS (
    SELECT
        account_id,
        substr(invoice_date, 1, 7) AS metric_month,
        SUM(amount_usd) AS invoiced_amount_usd,
        AVG(discount_pct) AS avg_discount_pct,
        AVG(days_to_pay) AS avg_days_to_pay
    FROM raw_invoices
    GROUP BY 1, 2
)
SELECT
    m.metric_month,
    a.account_id,
    a.account_name,
    a.region,
    a.segment,
    a.acquisition_channel,
    a.signup_date,
    a.initial_arr_usd,
    m.arr_usd,
    m.active_flag,
    m.seats_purchased,
    m.active_seats,
    ROUND(CAST(m.active_seats AS REAL) / NULLIF(m.seats_purchased, 0), 4) AS utilization_pct,
    m.feature_adoption_pct,
    m.support_tickets,
    m.csat_score,
    m.renewal_due_in_days,
    COALESCE(i.invoiced_amount_usd, 0) AS invoiced_amount_usd,
    COALESCE(i.avg_discount_pct, 0) AS avg_discount_pct,
    COALESCE(i.avg_days_to_pay, 0) AS avg_days_to_pay,
    LAG(m.arr_usd) OVER (PARTITION BY a.account_id ORDER BY m.metric_month) AS prior_arr_usd,
    LAG(m.active_seats) OVER (PARTITION BY a.account_id ORDER BY m.metric_month) AS prior_active_seats
FROM raw_monthly_lifecycle_metrics m
JOIN raw_accounts a ON a.account_id = m.account_id
LEFT JOIN invoice_monthly i
    ON i.account_id = m.account_id
   AND i.metric_month = m.metric_month;
