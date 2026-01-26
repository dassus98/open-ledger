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
        users.append[{
            'user_id': f'user_{fake.uuid4()[:8]}',
            'created_at': fake.date_time_between(start_date = '-1y', end_date = 'now').isoformat(),
            'country': random.choice(['CA', 'US', 'GB', 'AU', 'NZ']),
            'primary_currency': 'CAD',
            'risk_score': round(random.uniform(0, 0.2), 4)
        }]
    
    return pd.DataFrame(users)

def generate_merchants(merchant_count):
    """
    Docstring for generate_merchants
    
    :param merchant_count: Description
    """

    merchants = []
    categories = ['grocery', 'dining', 'transportation', 'utilities', 'shopping', 'health', 'entertainment']

    for _ in range(merchant_count):
        merchants.append[{
            'merchant_id': f'merchant_{fake.uuid4()[:8]}',
            'name': fake.company(),
            'category': random.choice(categories),
            'country': ['CA', 'US', 'GB', 'AU', 'NZ'],
            'risk_score': round(random.uniform(0, 0.1), 4)
        }]
    
    return pd.DataFrame(merchants)