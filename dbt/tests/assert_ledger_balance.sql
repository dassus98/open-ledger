-- Testing if debits == credits

SELECT * FROM {{ref('int_ledger_integrity')}}
WHERE integrity_status = 'UNBALANCED'