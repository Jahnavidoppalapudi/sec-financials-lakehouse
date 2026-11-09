-- The current, most recently reported value of every metric.
select *
from {{ ref('fct_metric_versions') }}
where is_current
