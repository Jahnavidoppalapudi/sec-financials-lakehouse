-- Map XBRL concepts to standard metrics. Companies tag the same idea differently
-- (e.g. "Revenues" vs "RevenueFromContractWithCustomer..."), and switch tags over time.
-- For each company, metric and period we use the highest-priority concept it reported,
-- consistently across every filing, so versions of a number are always comparable.
with facts as (
    select * from {{ ref('stg_sec__facts') }}
    where period_length in ('instant', 'quarter', 'annual')
),

mapped as (
    select
        f.*,
        m.metric,
        m.priority
    from facts as f
    inner join {{ ref('concept_map') }} as m
        on f.taxonomy = m.taxonomy
        and f.concept = m.concept
        and f.unit = m.unit
        and f.period_type = m.period_type
),

ranked as (
    select
        *,
        min(priority) over (
            partition by company_key, metric, period_start, period_end
        ) as best_priority
    from mapped
)

select
    company_key,
    metric,
    concept,
    unit,
    period_length,
    period_start,
    period_end,
    value,
    filed_date,
    accession_number,
    form,
    source_cik
from ranked
where priority = best_priority
