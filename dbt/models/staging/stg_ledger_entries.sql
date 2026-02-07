{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('openledger', 'ledger_entries') }}
),

cleaned AS (
    SELECT
        TRIM(entry_id) AS entry_id,
        TRIM(transaction_id) AS transaction_id,
        TRIM(account_id) AS account_id,
        
        -- Standardize types
        LOWER(TRIM(account_type)) AS account_type,
        LOWER(TRIM(entry_type)) AS entry_type,
        
        -- Safe conversions
        TRY_CAST(amount AS NUMBER(18, 2)) AS amount,
        TRY_TO_TIMESTAMP(event_time) AS event_time

    FROM source
)

SELECT * FROM cleaned