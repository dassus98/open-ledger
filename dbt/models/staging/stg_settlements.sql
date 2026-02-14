{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('openledger', 'settlements') }}
),

cleaned AS (
    SELECT
        TRIM(settlement_id) AS settlement_id,
        TRIM(transaction_id) AS transaction_id,
        TRIM(merchant_id) AS merchant_id,

        -- Financials
        CAST(gross_amount AS NUMBER(18, 2)) AS gross_amount,
        CAST(fee_amount AS NUMBER(18, 2)) AS fee_amount,
        CAST(net_amount AS NUMBER(18, 2)) AS net_amount,
        TRIM(currency) AS currency,

        -- Dates & Status
        settlement_date,
        TRIM(processor_reference) AS processor_reference,
        LOWER(TRIM(status)) AS status,
        TRIM(discrepancy_reason) AS discrepancy_reason

    FROM source
)

SELECT * FROM cleaned