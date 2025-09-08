{{ config(materialized='table') }}

with experiment_data as (
    select * from {{ ref('int_experiment_orders_union') }}
),

order_level_analysis as (
    select
        -- Primary keys
        order_id,
        user_id,
        
        -- Experiment metadata
        data_source,
        experiment_name,
        variant,
        control_treatment,
        variant_description,
        experiment_status,
        
        -- Time period classification
        case 
            when created_at >= '{{ var("free_shipping_threshold_v1_1_1_experiment_start_date") }}' 
                 and created_at < '{{ var("free_shipping_threshold_v1_1_1_experiment_end_date") }}' then 'experiment_period'
            when created_at >= '{{ var("free_shipping_threshold_v1_1_1_baseline_start_date") }}' 
                 and created_at < '{{ var("free_shipping_threshold_v1_1_1_baseline_end_date") }}' then 'baseline_period' 
            else 'other'
        end as time_period,
        
        -- Order attributes
        status,
        gender,
        num_of_item,
        
        -- Timestamps
        created_at,
        shipped_at,
        delivered_at,
        
        -- Derived metrics
        case when status = 'Complete' then 1 else 0 end as is_completed,
        case when status = 'Cancelled' then 1 else 0 end as is_cancelled,
        0 as is_returned,  -- All returns are null, hardcode to 0
        
        -- Timing metrics
        date_diff(shipped_at, created_at, day) as days_to_ship,
        date_diff(delivered_at, created_at, day) as days_to_deliver,
        
        -- Analysis metadata
        current_timestamp() as analysis_run_at,
        _loaded_at
        
    from experiment_data
    where created_at >= '{{ var("free_shipping_threshold_v1_1_1_baseline_start_date") }}'
          and created_at < '{{ var("free_shipping_threshold_v1_1_1_experiment_end_date") }}'
)

select * from order_level_analysis