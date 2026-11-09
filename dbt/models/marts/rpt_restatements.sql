-- Every time a company changed a number it had already reported.
select
    company_key,
    metric,
    period_length,
    period_start,
    period_end,
    version_number,
    first_reported_value,
    lag(value) over (
        partition by company_key, metric, period_start, period_end
        order by version_number
    ) as previous_value,
    value as restated_value,
    round(100.0 * (value - first_reported_value) / nullif(abs(first_reported_value), 0), 2)
        as change_vs_first_pct,
    known_from as restated_on,
    accession_number,
    form
from {{ ref('fct_metric_versions') }}
qualify version_number > 1
