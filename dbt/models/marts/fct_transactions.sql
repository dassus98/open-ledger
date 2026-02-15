{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='fail'
) }}

WITH internal AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

external AS (
    SELECT * FROM {{ ref('stg_settlements') }}
),

integrity AS (
    SELECT transaction_id, integrity_status
    FROM {{ref('int_ledger_integrity')}}
),

joined AS (
    SELECT
        -- COALESCE ensures we keep the ID even if it's missing from one side
        COALESCE(i.transaction_id, e.transaction_id) AS transaction_id,
        
        -- Dimensions
        COALESCE(i.user_id, 'UNKNOWN') AS user_id,
        COALESCE(i.merchant_id, e.merchant_id) AS merchant_id,
        COALESCE(i.event_time, e.settlement_date) AS transaction_at,
        e.settlement_date,
        
        -- Metrics
        i.amount AS internal_amount,
        e.gross_amount AS bank_gross_amount,
        e.net_amount AS bank_settled_amount,
        e.fee_amount AS bank_fee,

        -- Integrity check
        COALESCE(int.integrity_status, 'UNKNOWN') AS ledger_integrity,
        
        -- Checking if correct amount was charged
        ABS(ZEROIFNULL(i.amount) - ZEROIFNULL(e.gross_amount)) AS gross_match_delta,

        -- Should equal Gross - Fee
        ABS(ZEROIFNULL(e.gross_amount) - ZEROIFNULL(e.fee_amount) - ZEROIFNULL(e.net_amount)) AS settlement_math_delta
        
    FROM internal i
    FULL OUTER JOIN external e 
        ON i.transaction_id = e.transaction_id
    LEFT JOIN integrity int
        ON i.transaction_id = int.transaction_id
)

SELECT
    *,
    CASE
        WHEN internal_amount IS NOT NULL AND bank_settled_amount IS NULL THEN 'MISSING_EXTERNAL'
        WHEN internal_amount IS NULL AND bank_settled_amount IS NOT NULL THEN 'MISSING_INTERNAL'
        WHEN gross_match_delta > {{ var('reconciliation_tolerance') }} THEN 'GROSS_MISMATCH'
        WHEN settlement_math_delta > {{ var('reconciliation_tolerance') }} THEN 'SETTLEMENT_ERROR'
        WHEN e.status = 'failed' THEN 'SETTLEMENT_FAILED'
        ELSE 'MATCHED'
    END AS reconciliation_status
FROM joined

{% if is_incremental() %}
    WHERE transaction_at >= (SELECT MAX(transaction_at) FROM {{this}})
{% endif %}