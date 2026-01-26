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
    Docstring for generate_users
    
    :param user_count: Description
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