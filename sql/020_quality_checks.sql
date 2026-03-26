-- Check that no cohort retention percentage exceeds 100%.
SELECT *
FROM mart_cohort_retention
WHERE retention_pct > 1.0;

-- Check for negative ARR movement values in the revenue bridge.
SELECT *
FROM mart_revenue_bridge
WHERE new_arr_usd < 0
   OR expansion_arr_usd < 0
   OR contraction_arr_usd < 0
   OR churn_arr_usd < 0;

-- Check for missing segment coverage in the latest month.
SELECT *
FROM mart_segment_performance
WHERE metric_month = (SELECT MAX(metric_month) FROM mart_segment_performance)
  AND active_accounts = 0;

-- Check whether any renewal bucket has intervention counts above account counts.
SELECT *
FROM mart_renewal_pipeline
WHERE intervention_accounts > accounts_due;
