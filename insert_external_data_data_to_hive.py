import pandas as pd
from pyhive import hive
import requests as req

# Fetch data from the external_data API
external_data_api = "http://127.0.0.1:5000/api/external_data"
external_data_response = req.get(external_data_api)

if external_data_response.status_code == 200:
    external_data = external_data_response.json()
    external_data_df = pd.DataFrame(external_data)

    with hive.connect(host='localhost', database='transactions_data') as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions_data.external_data (
                    blacklist_info ARRAY<STRING>,
                    credit_scores MAP<STRING, INT>,
                    fraud_reports MAP<STRING, INT>
                )
            """)

            for _, row in external_data_df.iterrows():
                cursor.execute(f"""
                    INSERT INTO transactions_data.external_data
                    VALUES (ARRAY{row['blacklist_info']}, MAP{row['credit_scores']}, MAP{row['fraud_reports']})
                """)
else:
    print("Failed to fetch data from the external_data API.")
