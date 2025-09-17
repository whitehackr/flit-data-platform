{% macro generate_flatten_json(table_ref, json_column='json_body') %}

{% set get_json_paths %}
with sample_data as (
  select
    {{ json_column }},
    parse_json({{ json_column }}) as json_obj
  from {{ table_ref }}
  limit 1
),
json_flattened as (
  select
    json_path,
    -- Use modern BigQuery JSON subscript operator for dynamic access
    json_type(json_obj[json_path]) as data_type
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
  {% if dtype == 'NUMERIC' %}
    lax_int64(parse_json({{ json_column }})['{{ path }}']) as {{ path }}
  {% elif dtype == 'BOOLEAN' %}
    lax_bool(parse_json({{ json_column }})['{{ path }}']) as {{ path }}
  {% else %}
    lax_string(parse_json({{ json_column }})['{{ path }}']) as {{ path }}
  {% endif %}
  {%- if not loop.last -%},{%- endif %}
{% endfor %}

{% endmacro %}