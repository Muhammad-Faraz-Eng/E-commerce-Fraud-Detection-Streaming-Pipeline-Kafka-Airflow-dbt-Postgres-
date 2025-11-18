{{ config(
    materialized='table'
) }}

with t as (
    select * from {{ ref('stg_transactions') }}
)

select distinct
    merchant_id,
    merchant_category
from t