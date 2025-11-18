{{ config(
    materialized='incremental',
    unique_key='merchant_id'
) }}

SELECT
    DISTINCT merchant_id,
    merchant_category
FROM {{ ref('stg_transactions') }}

{% if is_incremental() %}
WHERE merchant_id NOT IN (SELECT merchant_id FROM {{ this }})
{% endif %}
