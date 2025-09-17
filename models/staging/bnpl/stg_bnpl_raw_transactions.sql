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

        -- Original transaction ID (resets daily)
        json_extract_scalar(json_body, '$.transaction_id') as original_transaction_id,

        -- Core transaction fields
        json_extract_scalar(json_body, '$.customer_id') as customer_id,
        json_extract_scalar(json_body, '$.product_id') as product_id,
        cast(json_extract_scalar(json_body, '$.amount') as numeric) as amount,
        json_extract_scalar(json_body, '$.currency') as currency,
        json_extract_scalar(json_body, '$.status') as status,

        -- Risk and ML fields
        cast(json_extract_scalar(json_body, '$.risk_score') as float64) as risk_score,
        json_extract_scalar(json_body, '$.risk_level') as risk_level,
        cast(json_extract_scalar(json_body, '$.will_default') as boolean) as will_default,

        -- Payment method
        json_extract_scalar(json_body, '$.payment_method_id') as payment_method_id,

        -- Timestamps
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