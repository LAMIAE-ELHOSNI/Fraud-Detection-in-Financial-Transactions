import pandas as pd
from pyhive import hive
import requests as req

# Fetch data from the transactions API
transactions_api = "http://127.0.0.1:5000/api/transactions"
transactions_response = req.get(transactions_api)

if transactions_response.status_code == 200:
    transactions_data = transactions_response.json()
    transactions_df = pd.DataFrame(transactions_data)

    with hive.connect(host='localhost', database='transactions_data') as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions_data.transactions (
                    amount DOUBLE,
                    currency STRING,
                    customer_id STRING,
                    date_time STRING,
                    location STRING,
                    merchant_details STRING,
                    transaction_id STRING,
                    transaction_type STRING
                )
            """)

            for _, row in transactions_df.iterrows():
                cursor.execute(f"""
                    INSERT INTO transactions_data.transactions
                    VALUES ({row['amount']}, '{row['currency']}', '{row['customer_id']}',
                            '{row['date_time']}', '{row['location']}', '{row['merchant_details']}',
                            '{row['transaction_id']}', '{row['transaction_type']}')
                """)
else:
    print("Failed to fetch data from the transactions API.")
