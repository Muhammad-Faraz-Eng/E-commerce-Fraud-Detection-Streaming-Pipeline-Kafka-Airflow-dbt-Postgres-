{{config(
    materialized='incremental',
    unique_key='transaction_id'
)}}

with bronze as(
    select * from {{ref('raw_transactions')}}
)


select
    transaction_id,
    customer_id,
    device_id,
    merchant_id,
    transaction_amount,
    currency,
    transaction_timestamp,
    payment_method,
    transaction_status,
    transaction_type,
    fraud_score,
    is_fraud,
    chargeback_flag,
    velocity_risk_flag,
    geolocation_risk_flag,
    device_risk_flag,
    payment_method_risk_flag,
    account_risk_flag,
    external_blacklist_flag,
    cart_value,
    promotion_id,
    merchant_category,
    coupon_code_used,
    received_at
from bronze

{% if is_incremental() %}
WHERE received_at > (SELECT MAX(received_at) from {{ this }})
{% endif %}