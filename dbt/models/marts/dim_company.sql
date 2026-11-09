-- One row per company, listing every legal entity (CIK) that reported for it.
with entities as (
    select
        company_key,
        source_cik,
        any_value(entity_name) as entity_name,
        min(period_end) as first_period_end,
        max(filed_date) as last_filed_date
    from {{ source('silver', 'facts') }}
    group by company_key, source_cik
)

select
    company_key,
    arg_max(entity_name, last_filed_date) as current_entity_name,
    arg_max(source_cik, last_filed_date) as current_cik,
    string_agg(source_cik, ', ' order by last_filed_date) as all_ciks,
    count(*) as entity_count,
    min(first_period_end) as history_starts,
    max(last_filed_date) as last_filed_date
from entities
group by company_key
