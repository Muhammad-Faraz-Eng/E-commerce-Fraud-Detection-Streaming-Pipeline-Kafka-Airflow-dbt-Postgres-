{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

SELECT
    t.transaction_id,
    t.transaction_amount,
    t.currency,
    t.transaction_timestamp,
    t.payment_method,
    t.transaction_status,
    t.transaction_type,
    t.fraud_score,
    t.is_fraud,
    t.chargeback_flag,
    t.velocity_risk_flag,
    t.geolocation_risk_flag,
    t.device_risk_flag,
    t.payment_method_risk_flag,
    t.account_risk_flag,
    t.external_blacklist_flag,
    t.cart_value,
    t.promotion_id,
    t.coupon_code_used,
    c.customer_id,
    m.merchant_id
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('dim_customer') }} c 
    ON t.customer_id::text = c.customer_id::text 
    AND c.current_flag = TRUE
LEFT JOIN {{ ref('dim_merchant') }} m 
    ON t.merchant_id::text = m.merchant_id::text

{% if is_incremental() %}
WHERE t.transaction_id NOT IN (SELECT transaction_id FROM {{ this }})
{% endif %}
