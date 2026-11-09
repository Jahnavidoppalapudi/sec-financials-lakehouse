-- Annual and quarterly report facts only, with each period classified by length.
with source as (
    select * from {{ source('silver', 'facts') }}
),

classified as (
    select
        company_key,
        source_cik,
        entity_name,
        taxonomy,
        concept,
        unit,
        period_start,
        period_end,
        value,
        accession_number,
        form,
        filed_date,
        case when period_start is null then 'instant' else 'duration' end as period_type,
        datediff('day', period_start, period_end) + 1 as period_days
    from source
    where form in ('10-K', '10-K/A', '10-KT', '10-Q', '10-Q/A', '10-QT')
      and filed_date is not null
)

select
    *,
    case
        when period_type = 'instant' then 'instant'
        when period_days between 80 and 100 then 'quarter'
        when period_days between 350 and 380 then 'annual'
        else 'other'
    end as period_length
from classified
