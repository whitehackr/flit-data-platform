{{ config(materialized='view') }}

with source as (
    select * from {{ var('thelook_dataset') }}.orders
),

renamed as (
    select
        -- Primary keys
        order_id,
        user_id,
        
        -- Experiment metadata (null for baseline data)
        cast(null as string) as experiment_name,
        cast(null as string) as variant,
        
        -- Order details  
        status,
        gender,
        num_of_item,
        
        -- Timestamps
        created_at,
        shipped_at,
        delivered_at,
        returned_at,
        
        -- Data lineage
        'thelook_baseline' as data_source,
        current_timestamp() as _loaded_at
        
    from source
)

select * from renamed