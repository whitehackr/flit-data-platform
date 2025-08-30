{{ config(materialized='table') }}

with baseline_data as (
    select * from {{ ref('stg_thelook_baseline_orders') }}
    where created_at >= '{{ var("free_shipping_threshold_v1_1_1_baseline_start_date") }}'
    and created_at < '{{ var("free_shipping_threshold_v1_1_1_baseline_end_date") }}'
),

synthetic_overlay as (
    select * from {{ ref('stg_synthetic_overlay_orders') }}
),

unioned as (
    select * from baseline_data
    union all
    select * from synthetic_overlay
)

select * from unioned