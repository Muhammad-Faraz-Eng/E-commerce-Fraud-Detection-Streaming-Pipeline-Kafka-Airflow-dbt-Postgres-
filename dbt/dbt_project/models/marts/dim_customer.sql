{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

-- CTE for new incoming data
WITH new_data AS (
    SELECT
        customer_id::bigint AS customer_id,   -- CAST to bigint
        customer_email,
        customer_phone,
        customer_location,
        customer_account_created,
        customer_risk_score,
        customer_previous_fraud,
        CURRENT_TIMESTAMP AS load_date,
        TRUE AS current_flag
    FROM {{ ref('stg_customers') }}
),

-- CTE for existing data in the target table (only for incremental runs)
existing AS (
    {% if is_incremental() %}
    SELECT * FROM {{ this }}
    {% else %}
    SELECT 
        NULL::bigint AS customer_id,
        NULL::varchar AS customer_email,
        NULL::varchar AS customer_phone,
        NULL::varchar AS customer_location,
        NULL::timestamp AS customer_account_created,
        NULL::numeric AS customer_risk_score,
        NULL::boolean AS customer_previous_fraud,
        NULL::timestamp AS load_date,
        NULL::boolean AS current_flag
    {% endif %}
)

-- Final selection with incremental logic
SELECT
    n.customer_id,
    n.customer_email,
    n.customer_phone,
    n.customer_location,
    n.customer_account_created,
    n.customer_risk_score,
    n.customer_previous_fraud,
    n.load_date,
    n.current_flag
FROM new_data n
LEFT JOIN existing e
    ON n.customer_id = e.customer_id
{% if is_incremental() %}
WHERE e.customer_id IS NULL
   OR n.customer_email != e.customer_email
   OR n.customer_phone != e.customer_phone
   OR n.customer_location != e.customer_location
   OR n.customer_risk_score != e.customer_risk_score
   OR n.customer_previous_fraud != e.customer_previous_fraud
{% endif %}
