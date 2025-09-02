{{ config(materialized='view') }}

with source as (
    select * from {{ var('raw_dataset') }}.synthetic_free_shipping_threshold_test_v1_1_1_orders
),

renamed as (
    select
        -- Primary keys
        order_id,
        user_id,
        
        -- Experiment metadata
        experiment_name,
        variant,
        
        -- Order details
        status,
        gender,
        num_of_item,
        
        -- Timestamps
        created_at,
        shipped_at,
        delivered_at,
        
        -- Data lineage
        'synthetic_overlay' as data_source,
        current_timestamp() as _loaded_at
        
    from source
)

select * from renamed