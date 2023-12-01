import requests
from pyhive import hive
# API endpoint
api_url = 'http://127.0.0.1:5000/api/transactions'
# Hive connection settings
hive_host = 'localhost'
hive_port = 10000
hive_database = 'Transactions_Data'
hive_table = 'Table_Transactions'

# Create a Hive connection
conn = hive.Connection(host=hive_host, port=hive_port, database=hive_database)
# Create a cursor
cursor = conn.cursor()

# Fetch data from the API
response = requests.get(api_url)
api_data = response.json()

# Define the Hive table schema
hive_table_schema = """
CREATE TABLE IF NOT EXISTS {table} (
    amount DOUBLE,
    currency STRING,
    customer_id STRING,
    date_time STRING,
    location STRING,
    merchant_details STRING,
    transaction_id STRING,
    transaction_type STRING
)
"""
# Execute the Hive table creation query
cursor.execute(hive_table_schema.format(table=hive_table))
# Insert data into Hive table
hive_insert_query = f"INSERT INTO TABLE {hive_table} VALUES %s"
# Prepare the data for insertion
data_to_insert = ", ".join([f"({entry['amount']}, '{entry['currency']}', '{entry['customer_id']}', '{entry['date_time']}', '{entry['location']}', '{entry['merchant_details']}', '{entry['transaction_id']}', '{entry['transaction_type']}')" for entry in api_data])
# Execute the Hive insert query with the prepared data
cursor.execute(hive_insert_query % data_to_insert)
# Commit the transaction!   =
conn.commit()
# Close the Hive connection
conn.close()
