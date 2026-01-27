
  
    

        create or replace transient table OPENLEDGER.ANALYTICS.fct_transactions
         as
        (

WITH internal AS (
    SELECT * FROM OPENLEDGER.ANALYTICS.staging_transactions
),

external AS (
    SELECT * FROM OPENLEDGER.ANALYTICS.staging_settlements
),

joined AS (
    SELECT
        -- COALESCE ensures we keep the ID even if it's missing from one side
        COALESCE(i.transaction_id, e.transaction_id) AS transaction_id,
        
        -- Dimensions
        i.user_id,
        i.merchant_id,
        i.event_time AS transaction_at,
        e.settlement_date,
        
        -- Metrics
        i.amount AS internal_amount,
        e.net_amount AS bank_settled_amount,
        e.fee_amount AS bank_fee,
        
        -- Calculating the difference (0 == perfect match)
        -- Handling null values by treating them as 0
        ZEROIFNULL(e.net_amount) - ZEROIFNULL(i.amount) AS reconciliation_delta
        
    FROM internal i
    FULL OUTER JOIN external e 
        ON i.transaction_id = e.transaction_id
)

SELECT
    *,
    CASE
        WHEN internal_amount IS NOT NULL AND bank_settled_amount IS NULL THEN 'MISSING_EXTERNAL'
        WHEN internal_amount IS NULL AND bank_settled_amount IS NOT NULL THEN 'MISSING_INTERNAL'
        WHEN internal_amount = bank_settled_amount THEN 'MATCHED'
        ELSE 'DISCREPANCY'
    END AS reconciliation_status
FROM joined
        );
      
  