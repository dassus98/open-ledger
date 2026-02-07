WITH ledger_entries AS (
    SELECT * FROM {{ref('stg_ledger_entries')}}
),

transaction_balances AS (
    SELECT
        transaction_id,
        SUM(CASE WHEN entry_type = 'DEBIT' THEN amount ELSE 0 END) AS total_debits,
        SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE 0 END) AS total_credits,
        COUNT(*) AS entry_count
    FROM ledger_entries
    GROUP BY transaction_id
)

SELECT
    transaction_id,
    total_debits,
    total_credits,
    ABS(total_debits - total_credits) AS balance_delta,
    CASE
        WHEN ABS(total_debits - total_credits) < 0.01 THEN 'BALANCED'
        ELSE 'UNBALANCED'
    END AS integrity_status
FROM transaction_balances