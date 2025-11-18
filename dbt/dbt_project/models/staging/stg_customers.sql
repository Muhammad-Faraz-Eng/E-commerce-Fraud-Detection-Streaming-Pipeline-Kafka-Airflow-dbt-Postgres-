-- stg_customers.sql
{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge'
) }}

WITH bronze AS (
    SELECT 
        customer_id,
        customer_email,
        customer_phone,
        customer_location,
        customer_account_created,
        customer_risk_score,
        customer_previous_fraud,
        received_at 
    FROM {{ ref('raw_transactions') }}
)
{% if is_incremental() %}
, last AS (
    SELECT MAX(received_at) AS max_received_at FROM {{ this }}
)
{% endif %}

SELECT DISTINCT ON (customer_id)
    customer_id,
    customer_email,
    customer_phone,
    customer_location,
    customer_account_created,
    customer_risk_score,
    customer_previous_fraud,
    received_at
FROM bronze b
{% if is_incremental() %}
CROSS JOIN last
WHERE b.received_at > COALESCE(last.max_received_at, '1900-01-01'::timestamp)
{% endif %}
ORDER BY customer_id, received_at DESC