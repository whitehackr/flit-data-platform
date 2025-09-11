{{ config(materialized='table') }}

with baseline_data as (
    -- original orders without experiment treatment
    select * from {{ ref('stg_thelook_baseline_orders') }}
    where created_at >= '{{ var("free_shipping_threshold_v1_1_1_baseline_start_date") }}'
    and created_at <= '{{ var("free_shipping_threshold_v1_1_1_experiment_end_date") }}'
),

synthetic_overlay as (
    -- simulated treatment effects, added as an overlay
    select * from {{ ref('stg_synthetic_overlay_orders') }}
),

experiment_assignments as (
    select 
        cast(object_identifier as int64) as user_id, -- in this experiment, the object is the user
        experiment_name,
        variant as control_treatment,
        variant_description
    from {{ ref('stg_experiment_assignments') }}
    where experiment_name = 'free_shipping_threshold_test_v1_1_1'
),

unioned as (
    select * from baseline_data
    union all
    select * from synthetic_overlay
),

final as (
    select 
        u.*,
        a.control_treatment,
        a.variant_description,
        case 
            when a.control_treatment is not null then 'experiment_participant'
            else 'non_participant'
        end as experiment_status
    from unioned u
    left join experiment_assignments a 
        on u.user_id = a.user_id
)

select * from final