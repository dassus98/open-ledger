import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

fake = Faker()
Faker.seed(42)

user_count = 100
merchant_count = 20
start_date = datetime(2025, 1, 1)
day_count = 30

def generate_users(user_count):
    """
    Generates fake users as clients of a bank. A beta distribution with a high beta would be most realistic for this data. But since this project is not trying to generate millions of rows of data, a uniform distribution will be used for convenience.
    
    :param user_count: Count of users generated for the experiment.
    """

    users = []

    for _ in range(user_count):
        users.append({
            'user_id': f'user_{fake.uuid4()[:8]}',
            'created_at': fake.date_time_between(start_date = '-1y', end_date = 'now').isoformat(),
            'country': random.choice(['CA', 'US', 'GB', 'AU', 'NZ']),
            'primary_currency': 'CAD',
            'risk_score': round(random.uniform(0, 0.2), 4)
        })
    
    return pd.DataFrame(users)

def generate_merchants(merchant_count):
    """
    Docstring for generate_merchants
    
    :param merchant_count: Description
    """

    merchants = []
    categories = ['grocery', 'dining', 'transportation', 'utilities', 'shopping', 'health', 'entertainment']

    for _ in range(merchant_count):
        merchants.append({
            'merchant_id': f'merchant_{fake.uuid4()[:8]}',
            'name': fake.company(),
            'category': random.choice(categories),
            'country': ['CA', 'US', 'GB', 'AU', 'NZ'],
            'risk_score': round(random.uniform(0, 0.1), 4)
        })
    
    return pd.DataFrame(merchants)

def generate_transactions(users_df, merchants_df, date):
    """
    Docstring for generate_transactions
    
    :param users_df: Description
    :param merchants_df: Description
    :param date: Description
    """

    transactions = []
    ledger_entries = []

    is_weekend = date.weekday() >= 5
    if is_weekend:
        daily_count = random.randint(50, 80)
    else:
        daily_count = random.randint(100, 150)

    for _ in range(daily_count):
        # Part I: Creating transactions
        user = users_df.sample(1).iloc[0]
        merchant = merchants_df.sample(1).iloc[0]
        transaction_id = f'transaction_{fake.uuid4()[:12]}'
        amount = round(random.uniform(5.0, 500.0), 2)

        transaction = {
            'transaction_id': transaction_id,
            'user_id': user['user_id'],
            'merchant_id': merchant['merchant_id'],
            'amount': amount,
            'currency': 'CAD',
            'transaction_type': 'purchase',
            'event_time': date + timedelta(hours = random.randint(6, 23), minutes = random.randint(0, 59)),
            'status': 'completed'
        }

        # Part II: Including bad data
        transaction_chaos = random.random()
        if transaction_chaos < 0.02:
            transaction['amount'] = None # Missing amount
        elif transaction_chaos <= 0.04:
            transaction['user_id'] = None # Missing user id
        elif transaction_chaos <= 0.05:
            if transaction['amount']:
                transaction['amount'] = transaction['amount'] * -1 # Negative transaction amount
        elif transaction_chaos <= 0.06:
            transaction['event_time'] = transaction['event_time'] + timedelta(days = 365) # One year into future
        elif transaction_chaos <= 0.07:
            transaction['currency'] = random.choice(['cad', '$CAD', 'USD', '$USD', 'AUD']) # Incorrect currency format
        elif transaction_chaos <= 0.08:
            transaction['merchant_id'] = f' {transaction['merchant_id']} ' # Hidden whitespace
        
        transactions.append(transaction)

        if transaction_chaos > 0.98:
            transactions.append(transaction.copy()) # Creating duplicates

        # Part III: Basic data contract
        # Establishing that debits == credits
        if transaction['amount'] and transaction['user_id']:
            ledger_entries.append({
                'entry_id': f'entry_{fake.uuid4()[:8]}',
                'transaction_id': transaction_id,
                'account_type': 'user',
                'account_id': user['user_id'],
                'entry_type': 'debit',
                'amount': amount,
                'event_time': transaction['event_time']
            })

            ledger_entries.append({
                'entry_id': f'entry_{fake.uuid4()[:8]}',
                'transaction_id': transaction_id,
                'account_type': 'merchant',
                'account_id': merchant['merchant_id'],
                'entry_type': 'credit',
                'amount': amount,
                'event_time': transaction['event_time']
            })

    return pd.DataFrame(transactions), pd.DataFrame(ledger_entries)
    

def generate_settlements(transactions_df):
    """
    Generates data from an external payment processor such as Stripe or VISA. The data here will be largely perfect but with minute errors built-in to demonstrate how dbt reconciliation logic can be triggered.
    
    :param transactions_df: Description
    """

    settlements = []

    valid_transactions = transactions_df[transactions_df['status'] == 'completed']

    for _, transaction in valid_transactions.iterrows():
        fee_rate = 0.02
        flat_fee = 0.30
        gross_amount = float(transaction['amount']) if transaction['amount'] else 0
        fee = round((gross_amount * fee_rate) + flat_fee, 2)
        net_amount = round(gross_amount - fee, 2)

        settlement_record = {
            'settlement_id': f'settlement_{fake.uuid4()[:12]}',
            'transaction_id': transaction['transaction_id'],
            'merchant_id': transaction['merchant_id'],
            'gross_amount': gross_amount,
            'fee_amount': fee,
            'net_amount': net_amount,
            'currency': transaction['currency'],
            'settlement_date': (pd.to_datetime(transaction['event_time']) + timedelta(days = random.randint(1, 3))).strftime('%Y-%m-%d'),
            'processor_reference': f'REF-{fake.swift(length = 8)}',
            'status': 'settled',
            'discrepancy_reason': None
        }

        settlement_chaos = random.random()

        if settlement_chaos < 0.02:
            settlement_record['net_amount'] -= 1.00 # Removing one dollar from the fee
            settlement_record['discrepancy_reason'] = 'amount_mismatch'
        elif settlement_chaos < 0.03:
            settlement_record['status'] = 'failed' # Failed for unknown reason
        elif settlement_chaos < 0.04:            
            continue # We have record, payment processor does not

        settlements.append(settlement_record)

    # Generating records that exist in payment processor, but not internally
    for _ in range(int(len(transactions_df) * 0.01)):
        settlements.append({
            'settlement_id': f'settlement_{fake.uuid4()[:12]}',
            'transaction_id': f'transaction_unknown_{fake.uuid4()[:8]}',
            'merchant_id': f'merchant_{fake.uuid4()[:8]}',
            'gross_amount': round(random.uniform(10, 200), 2),
            'fee_amount': 5.00,
            'net_amount': 0.00,
            'currency': 'CAD',
            'settlement_date': (pd.to_datetime(transactions_df['event_time'].max()) + timedelta(days=1)).strftime('%Y-%m-%d'),
            'processor_reference': f'REF-{fake.swift(length = 8)}',
            'status': 'settled',
            'discrepancy_reason': 'external_only'
        })
    
    return pd.DataFrame(settlements)
    
def main():
    print('Generating data...')
    users = generate_users(user_count)
    merchants = generate_merchants(merchant_count)

    # Creating csvs for now, in a real-world situation would be impossible
    # Parquet (columnar-format) or Avro (JSON) would be ideal
    # Parquet compresses data, can enforce a schema, and makes queries significantly faster
    os.makedirs('data/raw', exist_ok = True)
    users.to_csv('data/raw/users.csv', index = False)
    merchants.to_csv('data/raw/merchants.csv', index = False)

    all_transactions = []
    all_entries = []

    print(f'Generating transactions for {day_count} days...')
    for i in range(day_count):
        current_date = start_date + timedelta(days = i)
        transactions_df, entries_df = generate_transactions(users, merchants, current_date)
        all_transactions.append(transactions_df)
        all_entries.append(entries_df)

    pd.concat(all_transactions).to_csv('data/raw/transactions.csv', index = False)
    pd.concat(all_entries).to_csv('data/raw/entries.csv', index = False)

    print(f'Transaction generation complete. {sum(len(df) for df in all_transactions)} transactions have been created.')

    print('Generating settlements...')
    
    full_transactions_df = pd.concat(all_transactions)
    full_entries_df = pd.concat(all_entries)
    settlements_df = generate_settlements(full_transactions_df)

    # full_transactions_df.to_csv('data/raw/transactions.csv', index = False)
    full_entries_df.to_csv('data/raw/ledger_entries.csv', index = False)
    settlements_df.to_csv('data/raw/settlements.csv', index = False)

    print(f'{len(full_transactions_df)} transactions and {len(settlements_df)} settlements generated.')

if __name__ == '__main__':
    main()