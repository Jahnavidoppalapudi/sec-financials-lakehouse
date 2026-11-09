{#
  Every metric exactly as it was known on `as_of_date`.
  Usage: select * from ({{ metrics_as_of("'2024-03-01'") }}) where company_key = 'AAPL'
#}
{% macro metrics_as_of(as_of_date) %}
    select *
    from {{ ref('fct_metric_versions') }}
    where known_from <= cast({{ as_of_date }} as date)
      and (known_to is null or known_to > cast({{ as_of_date }} as date))
{% endmacro %}
