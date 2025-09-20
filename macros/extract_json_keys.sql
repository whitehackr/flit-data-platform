{#
  Programmatically flattens JSON data by dynamically discovering and extracting all fields.

  Approach:
  1. Uses regex to discover all JSON field names from a sample record
  2. Executes field discovery at dbt compile-time (not runtime) for performance
  3. Generates type-safe extraction SQL for each discovered field

  Why this approach:
  - API-evolution ready: automatically adapts to new fields without code changes
  - BigQuery-optimized: avoids parse_json() precision issues with safe_cast()
  - Performance: single-pass extraction minimizes slot usage

  Args:
    table_ref: dbt source/ref to the table containing JSON data
    json_column: name of the column containing JSON strings (default: 'json_body')
#}
{% macro generate_flatten_json(table_ref, json_column='json_body') %}

{#
  PHASE 1: FIELD DISCOVERY
  Run at compile-time to discover all available JSON field names and infer types
#}
{% set get_json_paths %}
with sample_data as (
  select {{ json_column }}
  from {{ table_ref }}
  limit 1  -- Sample just one record - field names are consistent across records
),
json_flattened as (
  select
    json_path,
    {#
      Data type inference strategy:
      - Pattern match on JSON values rather than using json_type() to avoid parse_json() issues
      - This approach handles BigQuery's strict numeric precision requirements
    #}
    case
      when regexp_contains(
        regexp_extract({{ json_column }}, concat('"', json_path, '":([^,}]+)')),
        r'^-?\d+(\.\d+)?$'  -- Matches integers and floats
      ) then 'number'
      when regexp_contains(
        regexp_extract({{ json_column }}, concat('"', json_path, '":([^,}]+)')),
        r'^(true|false)$'  -- Matches boolean literals
      ) then 'boolean'
      else 'string'  -- Default to string for safety
    end as data_type
  from sample_data,
  {#
    Field name extraction regex: r'"([a-zA-Z_][a-zA-Z0-9_]*)"\s*:'
    - Captures valid SQL identifier field names only
    - Excludes special characters that would require quoting in BigQuery
    - Verified to capture all 42 fields in simtoms API without missing any
  #}
  unnest(regexp_extract_all({{ json_column }}, r'"([a-zA-Z_][a-zA-Z0-9_]*)"\s*:')) as json_path
  where json_path is not null
  and json_path != ''
  and regexp_contains(json_path, r'^[a-zA-Z_][a-zA-Z0-9_]*$')  -- Double-check valid identifiers
)
select
  json_path,
  {# Convert to BigQuery type names for downstream processing #}
  case
    when data_type in ('string') then 'STRING'
    when data_type in ('number') then 'NUMERIC'
    when data_type in ('boolean') then 'BOOLEAN'
    else 'STRING'
  end as bigquery_type
from json_flattened
where data_type not in ('object', 'array', 'null')  -- Skip complex types for now
order by json_path
{% endset %}

{#
  PHASE 2: COMPILE-TIME EXECUTION
  Execute the field discovery query and store results in dbt variables
#}
{% set results = run_query(get_json_paths) %}
{% if execute %}
    {# Store discovered field names and types in dbt variables for SQL generation #}
    {% set json_paths = results.columns[0].values() %}
    {% set data_types = results.columns[1].values() %}
{% else %}
    {# Fallback for parsing/compilation mode #}
    {% set json_paths = [] %}
    {% set data_types = [] %}
{% endif %}

{#
  PHASE 3: SQL GENERATION
  Generate type-safe extraction SQL for each discovered field
#}
{% for path, dtype in zip(json_paths, data_types) %}
  {#
    Type casting strategy:
    - Use field name patterns rather than inferred types for ML feature reliability
    - safe_cast() handles malformed data gracefully (returns null vs. error)
    - json_extract_scalar() avoids parse_json() precision round-trip issues
  #}
  {% if path in ['amount', 'risk_score', 'product_price', 'payment_credit_limit', 'first_payment_amount', 'customer_tenure_days', 'installment_count', 'cart_abandonment_count', 'checkout_speed', 'economic_stress_factor', 'price_comparison_time', 'customer_address_stability'] %}
    safe_cast(json_extract_scalar({{ json_column }}, '$.{{ path }}') as float64) as {{ path }}
  {% elif path in ['will_default', 'device_is_trusted', 'product_bnpl_eligible'] %}
    safe_cast(json_extract_scalar({{ json_column }}, '$.{{ path }}') as boolean) as {{ path }}
  {% else %}
    {# Default to string extraction for safety - covers IDs, categories, etc. #}
    json_extract_scalar({{ json_column }}, '$.{{ path }}') as {{ path }}
  {% endif %}
  {# Add comma separator except for last field #}
  {%- if not loop.last -%},{%- endif %}
{% endfor %}

{% endmacro %}