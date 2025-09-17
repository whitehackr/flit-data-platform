{% macro generate_flatten_json(table_ref, json_column='json_body') %}

{% set get_json_paths %}
with sample_data as (
  select {{ json_column }}
  from {{ table_ref }}
  limit 1
),
json_flattened as (
  select
    json_path,
    -- Infer data type from the JSON string pattern - safer approach
    case
      when regexp_contains(
        regexp_extract({{ json_column }}, concat('"', json_path, '":([^,}]+)')),
        r'^-?\d+(\.\d+)?$'
      ) then 'number'
      when regexp_contains(
        regexp_extract({{ json_column }}, concat('"', json_path, '":([^,}]+)')),
        r'^(true|false)$'
      ) then 'boolean'
      else 'string'
    end as data_type
  from sample_data,
  unnest(regexp_extract_all({{ json_column }}, r'"([a-zA-Z_][a-zA-Z0-9_]*)"\s*:')) as json_path
  where json_path is not null
  and json_path != ''
  and regexp_contains(json_path, r'^[a-zA-Z_][a-zA-Z0-9_]*$')
)
select
  json_path,
  case
    when data_type in ('string') then 'STRING'
    when data_type in ('number') then 'NUMERIC'
    when data_type in ('boolean') then 'BOOLEAN'
    else 'STRING'
  end as bigquery_type
from json_flattened
where data_type not in ('object', 'array', 'null')
order by json_path
{% endset %}

{% set results = run_query(get_json_paths) %}
{% if execute %}
    {% set json_paths = results.columns[0].values() %}
    {% set data_types = results.columns[1].values() %}
{% else %}
    {% set json_paths = [] %}
    {% set data_types = [] %}
{% endif %}

{% for path, dtype in zip(json_paths, data_types) %}
  {% if path in ['amount', 'risk_score', 'product_price', 'payment_credit_limit', 'first_payment_amount', 'customer_tenure_days', 'installment_count', 'cart_abandonment_count', 'checkout_speed', 'economic_stress_factor', 'price_comparison_time', 'customer_address_stability'] %}
    safe_cast(json_extract_scalar({{ json_column }}, '$.{{ path }}') as float64) as {{ path }}
  {% elif path in ['will_default', 'device_is_trusted', 'product_bnpl_eligible'] %}
    safe_cast(json_extract_scalar({{ json_column }}, '$.{{ path }}') as boolean) as {{ path }}
  {% else %}
    json_extract_scalar({{ json_column }}, '$.{{ path }}') as {{ path }}
  {% endif %}
  {%- if not loop.last -%},{%- endif %}
{% endfor %}

{% endmacro %}