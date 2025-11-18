-- raw_transactions.sql
{{ config(
    materialized='table'
    ) }}

SELECT
    id,
    topic,
    {{ flatten_data('message', 'transaction_id') }} AS transaction_id,
    {{ flatten_data('message', 'customer_id') }} AS customer_id,
    {{ flatten_data('message', 'merchant_id') }} AS merchant_id,
    ({{ flatten_data('message', 'transaction_amount') }})::decimal AS transaction_amount,
    {{ flatten_data('message', 'currency') }} AS currency,
    ({{ flatten_data('message', 'timestamp') }})::timestamp AS transaction_timestamp,
    {{ flatten_data('message', 'payment_method') }} AS payment_method,
    {{ flatten_data('message', 'transaction_status') }} AS transaction_status,
    {{ flatten_data('message', 'transaction_type') }} AS transaction_type,
    
    {{ flatten_data('message', 'customer_email') }} AS customer_email,
    {{ flatten_data('message', 'customer_phone') }} AS customer_phone,
    {{ flatten_data('message', 'customer_location') }} AS customer_location,
    ({{ flatten_data('message', 'customer_account_created') }})::timestamp AS customer_account_created,
    ({{ flatten_data('message', 'customer_risk_score') }})::decimal AS customer_risk_score,
    ({{ flatten_data('message', 'customer_previous_fraud') }})::boolean AS customer_previous_fraud,
    
    {{ flatten_data('message', 'ip_address') }} AS ip_address,
    (message -> 'ip_geolocation')::jsonb AS ip_geolocation,
    {{ flatten_data('message', 'device_id') }} AS device_id,
    {{ flatten_data('message', 'device_type') }} AS device_type,
    {{ flatten_data('message', 'device_os') }} AS device_os,
    {{ flatten_data('message', 'browser') }} AS browser,
    ({{ flatten_data('message', 'is_vpn') }})::boolean AS is_vpn,
    
    ({{ flatten_data('message', 'login_attempts_last_24h') }})::int AS login_attempts_last_24h,
    ({{ flatten_data('message', 'failed_login_attempts') }})::int AS failed_login_attempts,
    ({{ flatten_data('message', 'password_change_recent') }})::boolean AS password_change_recent,
    ({{ flatten_data('message', 'account_age_days') }})::int AS account_age_days,
    ({{ flatten_data('message', 'transaction_count_last_7d') }})::int AS transaction_count_last_7d,
    ({{ flatten_data('message', 'average_transaction_amount_last_7d') }})::decimal AS average_transaction_amount_last_7d,
    ({{ flatten_data('message', 'shipping_address_mismatch') }})::boolean AS shipping_address_mismatch,
    
    ({{ flatten_data('message', 'is_fraud') }})::boolean AS is_fraud,
    ({{ flatten_data('message', 'fraud_score') }})::decimal AS fraud_score,
    ({{ flatten_data('message', 'chargeback_flag') }})::boolean AS chargeback_flag,
    ({{ flatten_data('message', 'velocity_risk_flag') }})::boolean AS velocity_risk_flag,
    ({{ flatten_data('message', 'geolocation_risk_flag') }})::boolean AS geolocation_risk_flag,
    ({{ flatten_data('message', 'device_risk_flag') }})::boolean AS device_risk_flag,
    ({{ flatten_data('message', 'payment_method_risk_flag') }})::boolean AS payment_method_risk_flag,
    ({{ flatten_data('message', 'account_risk_flag') }})::boolean AS account_risk_flag,
    ({{ flatten_data('message', 'external_blacklist_flag') }})::boolean AS external_blacklist_flag,
    
    {{ flatten_data('message', 'coupon_code_used') }} AS coupon_code_used,
    ({{ flatten_data('message', 'promotion_id') }})::int AS promotion_id,
    {{ flatten_data('message', 'merchant_category') }} AS merchant_category,
    {{ flatten_data('message', 'session_id') }} AS session_id,
    ({{ flatten_data('message', 'cart_value') }})::decimal AS cart_value,
    {{ flatten_data('message', 'referrer_url') }} AS referrer_url,
    {{ flatten_data('message', 'browser_fingerprint') }} AS browser_fingerprint,
    
    received_at
FROM raw.transactions_stream
