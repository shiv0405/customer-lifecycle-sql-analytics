# Metric Glossary

## Core Metrics

- `ARR`
  Annual recurring revenue active in the given month
- `utilization_pct`
  Active seats divided by purchased seats
- `feature_adoption_pct`
  Share of key product capabilities adopted by the account
- `gross_revenue_retention_pct`
  Current month retained ARR divided by prior month ARR before expansion
- `net_revenue_retention_pct`
  Current month ARR divided by prior month ARR including expansion and contraction
- `renewal_exposure_usd`
  ARR with renewal dates inside the next 90 days
- `risk_score`
  Composite score based on usage decline, support pressure, satisfaction, and renewal proximity

## Channel Metrics

- `cac_proxy_usd`
  Marketing spend divided by newly activated accounts
- `avg_first_year_arr_usd`
  Average ARR among accounts acquired through the channel
- `ninety_day_activation_pct`
  Share of newly acquired accounts that reach a healthy utilization threshold within 90 days

## Cohort Metrics

- `cohort_size`
  Number of accounts that signed in the same cohort month
- `retained_accounts`
  Accounts still carrying positive ARR in the measured month offset
- `retention_pct`
  Retained accounts divided by cohort size

## Planning Metrics

- `net_revenue_retention_proxy`
  Current segment ARR divided by prior month segment ARR
- `accounts_due`
  Count of active accounts in a renewal timing bucket
- `arr_due_usd`
  ARR value attached to the renewal bucket
- `intervention_accounts`
  Accounts inside the renewal bucket already showing low adoption, low satisfaction, or elevated ticket pressure
