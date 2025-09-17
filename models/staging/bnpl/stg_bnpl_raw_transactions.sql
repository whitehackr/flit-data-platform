{{ config(materialized='view') }}

{#
  BNPL Staging Model - Programmatic JSON Flattening

  This model solves two critical problems:
  1. Transaction ID uniqueness: API resets IDs daily, breaking primary key constraints
  2. Schema evolution: Automatically extracts all JSON fields without manual maintenance

  Design decisions:
  - Uses compile-time field discovery for performance (not runtime)
  - Preserves raw JSON for debugging and future field extraction
  - Generates composite unique IDs to handle API ID reset behavior
#}

with source as (
    select * from {{ source('flit_bnpl_raw', 'raw_bnpl_txs_json') }}
),

parsed as (
    select
        {#
          CRITICAL FIX: Transaction ID Uniqueness
          Problem: API resets transaction_id daily (txn_00000000 appears on multiple dates)
          Solution: Generate composite unique ID using transaction_id + customer_id + timestamp
        #}
        to_hex(md5(concat(
            json_extract_scalar(json_body, '$.transaction_id'),
            json_extract_scalar(json_body, '$.customer_id'),
            cast(_timestamp as string)
        ))) as unique_transaction_id,

        {#
          PROGRAMMATIC FIELD EXTRACTION
          This macro call generates 42 individual field extractions at compile-time.
          Benefits:
          - API evolution ready: new fields automatically discovered
          - Type safe: proper BigQuery casting for ML features
          - Performance: single-pass extraction
        #}
        {{ generate_flatten_json(source('flit_bnpl_raw', 'raw_bnpl_txs_json')) }},

        -- Raw table metadata preservation
        _timestamp as transaction_timestamp,
        _ingestion_timestamp,

        {#
          Always preserve raw JSON for:
          - Debugging data quality issues
          - Future field extraction needs
          - Schema evolution validation
        #}
        json_body,

        -- Data lineage tracking
        'bnpl_api' as data_source,
        current_timestamp() as _loaded_at

    from source
)

select * from parsed