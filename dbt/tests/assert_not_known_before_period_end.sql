{{ config(severity = 'warn') }}
-- A number cannot be known before the period it describes has ended.
-- Warn-only: some cover-page facts are dated close to the filing date.
select *
from {{ ref('fct_metric_versions') }}
where known_from < period_end
