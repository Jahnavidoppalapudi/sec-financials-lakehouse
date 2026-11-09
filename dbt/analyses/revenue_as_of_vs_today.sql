-- Annual revenue as known on 1 March 2024, compared with what is known today.
-- Compile with: dbt compile --select revenue_as_of_vs_today
with then_known as ({{ metrics_as_of("'2024-03-01'") }}),
     now_known as (select * from {{ ref('fct_metrics_latest') }})

select
    n.company_key,
    n.period_end,
    t.value as known_on_2024_03_01,
    n.value as known_today,
    t.value is distinct from n.value as changed_since
from now_known as n
left join then_known as t
    using (company_key, metric, period_start, period_end)
where n.metric = 'revenue' and n.period_length = 'annual'
order by n.company_key, n.period_end
