{{ config(
    materialized='incremental',
    unique_key='device_id'
) }}

SELECT
    device_id,
    device_type,
    device_os,
    browser,
    is_vpn,
    ip_address,
    ip_geolocation
FROM {{ ref('stg_devices') }}

{% if is_incremental() %}
WHERE device_id NOT IN (SELECT device_id FROM {{ this }})
{% endif %}
