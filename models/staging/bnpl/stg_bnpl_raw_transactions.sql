{{ config(materialized='view') }}

with source as (
    select * from {{ source('flit_bnpl_raw', 'raw_bnpl_txs_json') }}
),

parsed as (
    select
        -- Generate unique transaction ID to fix daily reset issue
        to_hex(md5(concat(
            json_extract_scalar(json_body, '$.transaction_id'),
            json_extract_scalar(json_body, '$.customer_id'),
            cast(_timestamp as string)
        ))) as unique_transaction_id,

        -- Extract all JSON fields dynamically
        {{ generate_flatten_json(source('flit_bnpl_raw', 'raw_bnpl_txs_json')) }},

        -- Timestamps from raw table
        _timestamp as transaction_timestamp,
        _ingestion_timestamp,

        -- Preserve full JSON for schema evolution and debugging
        json_body,

        -- Data lineage
        'bnpl_api' as data_source,
        current_timestamp() as _loaded_at

    from source
)

select * from parsed