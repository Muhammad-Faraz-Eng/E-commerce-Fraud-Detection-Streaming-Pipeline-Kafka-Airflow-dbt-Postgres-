{{ config(
    materialized='incremental',
    unique_key='transaction_id'
    ) }}

WITH bronze AS (
    SELECT * FROM {{ ref('raw_transactions') }}
)
{% if is_incremental() %}
, last AS (
    SELECT MAX(received_at) AS max_received_at FROM {{ this }}
)
{% endif %}

SELECT
    transaction_id,
    login_attempts_last_24h,
    failed_login_attempts,
    password_change_recent,
    account_age_days,
    transaction_count_last_7d,
    average_transaction_amount_last_7d,
    shipping_address_mismatch,
    received_at
FROM bronze b

{% if is_incremental() %}
CROSS JOIN last
WHERE b.received_at > COALESCE(last.max_received_at, '1900-01-01'::timestamp)
{% endif %}