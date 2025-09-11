{{ config(materialized='view') }}

select *
from {{ source('flit_raw', 'experiment_assignments') }}