-- Versions of a fact must form a clean timeline: each version ends exactly when the
-- next begins, with no gaps or overlaps, and exactly one current version.
with ordered as (
    select
        *,
        lead(known_from) over (
            partition by company_key, metric, period_start, period_end
            order by version_number
        ) as next_known_from,
        count(*) filter (where is_current) over (
            partition by company_key, metric, period_start, period_end
        ) as current_versions
    from {{ ref('fct_metric_versions') }}
)

select *
from ordered
where known_to is distinct from next_known_from
   or known_from >= coalesce(known_to, date '9999-12-31')
   or current_versions <> 1
