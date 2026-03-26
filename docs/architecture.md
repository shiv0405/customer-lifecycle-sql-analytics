# Architecture

## Overview

The project is organized like a lightweight analytics warehouse:

- source generation creates realistic customer, billing, usage, and marketing records
- raw CSVs are loaded into SQLite tables
- SQL views standardize the customer-month grain
- SQL marts produce reusable analytical outputs
- curated exports and written findings turn warehouse outputs into stakeholder-friendly analysis

## Layers

- `raw_*`
  Source extracts for accounts, monthly lifecycle metrics, invoices, and marketing spend
- `vw_account_monthly_health`
  Unified customer-month analysis layer
- `mart_*`
  Business-ready marts for retention, revenue movement, channel performance, region scorecards, and risk prioritization

## Analytical Grains

- account grain
  Customer attributes such as segment, region, acquisition channel, and signup month
- customer-month grain
  Product usage, support activity, satisfaction, ARR, and renewal timing
- reporting grain
  Region, segment, cohort, and renewal-window summaries used for recurring business reviews

## Analytical Pattern

1. Create realistic source records at customer and month grain.
2. Load sources into a local warehouse.
3. Build a clean customer-month fact view.
4. Use SQL window functions and cohort logic to derive mart-level metrics.
5. Export the marts into CSV, JSON, Markdown, and HTML outputs.
