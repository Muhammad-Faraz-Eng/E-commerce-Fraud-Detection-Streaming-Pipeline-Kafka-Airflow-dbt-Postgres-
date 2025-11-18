{{ config(
    materialized='incremental',
    unique_key='device_id',
    incremental_strategy='merge'
) }}

WITH bronze AS (
    SELECT 
        device_id,
        device_type,
        device_os,
        browser,
        is_vpn,
        ip_address,
        ip_geolocation,
        received_at 
    FROM {{ ref('raw_transactions') }}
)
{% if is_incremental() %}
, last AS (
    SELECT MAX(received_at) AS max_received_at FROM {{ this }}
)
{% endif %}

SELECT DISTINCT ON (device_id)
    device_id,
    device_type,
    device_os,
    browser,
    is_vpn,
    ip_address,
    ip_geolocation,
    received_at
FROM bronze b
{% if is_incremental() %}
CROSS JOIN last
WHERE b.received_at > COALESCE(last.max_received_at, '1900-01-01'::timestamp)
{% endif %}
ORDER BY device_id, received_at DESC