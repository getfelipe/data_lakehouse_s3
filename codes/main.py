from faker import Faker
from datetime import datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import uuid 
import random
import time


fake = Faker('pt_BR')
days = 25
registers_raw_path = '../datasets/raw_data/registers'
orders_raw_path = '../datasets/raw_data/orders'


def generate_register_data(n_rows=2000):
    data = []
    marital_status = ["Single", "Married", "Divorced", "Widowed"]
    for n in range(n_rows):
        data.append({
            'id': str(uuid.uuid4()),
            'name': fake.name(),
            'birth_date': fake.date_of_birth(minimum_age=18, maximum_age=70),
            'cpf': fake.cpf(),
            'postal_code': fake.postcode(),
            'country': 'Brazil',
            'city': fake.city(),
            'state': fake.state(),
            'address_street': fake.street_name(),
            'address_number': fake.building_number(),
            'gender': fake.random_element(elements=('M', 'F')),
            "marital_status": random.choice(marital_status),
            'phone': fake.phone_number(),
            'email': fake.email(),
            'register_date': fake.date_between(start_date='-1y', end_date='today')
        })

    return pd.DataFrame(data)


def generate_order_data(register_df, n_orders=5000):
    data = []

    # Creating a order
    for n in range(n_orders):
        customer_cpf = random.choice(register_df['cpf'])
        order_value = round(random.uniform(50, 1500), 2)
        charges = round(random.uniform(10, 150), 2)
        discount_value = random.choice([10, round(random.uniform(20, 200), 2)])
        voucher = fake.word()

        order_status = random.choices(
            ['invoiced', 'await_payment', 'canceled', 'pending', 'production'],
            weights=[60, 15, 5, 10, 10],
            k=1
        )[0]


        data.append({
            'order_id': str(uuid.uuid4()),
            'cpf': customer_cpf,
            'order_value': order_value,
            'charges': charges,
            'discount_value': discount_value,
            'voucher': voucher,
            'order_status': order_status,
            'order_date': fake.date_between(start_date='-1y', end_date='today')
        })

    return pd.DataFrame(data)
    


if __name__ == '__main__':

    for day in range(days):
        reference_date = (datetime.today() - timedelta(days=day)).strftime('%d%M%Y')
        df_register = generate_register_data()
        df_register_parquet = pa.Table.from_pandas(df_register)
        pq.write_table(df_register_parquet, f'{registers_raw_path}/registers_{reference_date}.parquet' )


        df_order = generate_order_data(df_register)
        df_order_parquet = pa.Table.from_pandas(df_order)
        pq.write_table(df_order_parquet, f'{orders_raw_path}/orders_{reference_date}.parquet')
