{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('openledger', 'transactions') }}
),

cleaned AS (
    SELECT
        -- IDs
        TRIM(transaction_id) AS transaction_id,
        TRIM(user_id) AS user_id,
        TRIM(merchant_id) AS merchant_id,

        -- Numeric Conversion (Handle empty strings safely)
        TRY_CAST(amount::VARCHAR AS DECIMAL(18, 2)) AS amount,
        TRIM(currency) AS currency,

        -- Status & Type
        LOWER(TRIM(transaction_type)) AS transaction_type,
        LOWER(TRIM(status)) AS status,

        TRY_TO_TIMESTAMP(event_time::VARCHAR) AS event_time

    FROM source
)

SELECT * FROM cleaned