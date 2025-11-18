{{ config(
    materialized='view'
) }}

SELECT
    c.customer_id,
    COUNT(f.transaction_id) AS total_transactions,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS total_fraud,
    AVG(f.fraud_score) AS avg_fraud_score,
    MAX(f.transaction_timestamp) AS last_transaction_date
FROM {{ ref('dim_customer') }} c
LEFT JOIN {{ ref('fct_transactions') }} f
    ON c.customer_id = f.customer_id
GROUP BY c.customer_id
