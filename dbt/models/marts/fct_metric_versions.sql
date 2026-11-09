-- Bitemporal fact table. One row per *version* of a reported number.
--   valid time:     period_start .. period_end   (what the number describes)
--   knowledge time: known_from .. known_to        (when the market knew this value)
-- A new version starts only when a later filing reports a different value; repeats of the
-- same value (e.g. comparative columns in next year's report) do not create versions.
with observations as (
    select * from {{ ref('int_metric_observations') }}
),

-- One value per fact per filing date (the latest filing that day wins)
per_day as (
    select *
    from observations
    qualify row_number() over (
        partition by company_key, metric, period_start, period_end, filed_date
        order by accession_number desc
    ) = 1
),

with_previous as (
    select
        *,
        lag(value) over (
            partition by company_key, metric, period_start, period_end
            order by filed_date
        ) as previous_value
    from per_day
),

change_points as (
    select * from with_previous
    where previous_value is null or value <> previous_value
)

select
    md5(concat_ws('|', company_key, metric, period_start, period_end, filed_date)) as version_key,
    company_key,
    metric,
    concept,
    unit,
    period_length,
    period_start,
    period_end,
    value,
    filed_date as known_from,
    lead(filed_date) over fact_window as known_to,
    row_number() over fact_window as version_number,
    row_number() over fact_window > 1 as is_restatement,
    lead(filed_date) over fact_window is null as is_current,
    first_value(value) over fact_window as first_reported_value,
    accession_number,
    form,
    source_cik
from change_points
window fact_window as (
    partition by company_key, metric, period_start, period_end
    order by filed_date
)
