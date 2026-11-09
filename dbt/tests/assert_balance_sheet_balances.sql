{{ config(severity = 'warn') }}
-- Accounting identity: total assets = total liabilities and equity (within 0.5%).
with latest as (
    select company_key, period_end, metric, value
    from {{ ref('fct_metrics_latest') }}
    where metric in ('total_assets', 'liabilities_and_equity')
),

pivoted as (
    select
        company_key,
        period_end,
        max(value) filter (where metric = 'total_assets') as total_assets,
        max(value) filter (where metric = 'liabilities_and_equity') as liabilities_and_equity
    from latest
    group by company_key, period_end
)

select *
from pivoted
where total_assets is not null
  and liabilities_and_equity is not null
  and abs(total_assets - liabilities_and_equity) > 0.005 * abs(total_assets)
