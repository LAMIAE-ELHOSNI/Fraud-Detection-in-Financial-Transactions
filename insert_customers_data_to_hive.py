from pyhive import hive 
import requests as req

# Get data from the API
transactions = req.get("http://127.0.0.1:5000/api/customers")

if transactions.status_code == 200:
    result = transactions.json()
    counter = 0
    while True:
        data = transactions.json()[counter]
        # Establish a connection
        connection = hive.connect(host='localhost', database='transactions_data')
        cursor = connection.cursor()
        
        # Create the table
        table_creation_query = """
            CREATE TABLE IF NOT EXISTS transactions_data.customers (
                account_history ARRAY<STRING>,
                avg_transaction_value DOUBLE,
                customer_id STRING,
                age INT,
                location STRING
            )
            """

        cursor.execute(table_creation_query)

        account_history_string = ",".join(data["account_history"])

        # Insert data into the table
        insert_query = '''
        INSERT INTO transactions_data.customers
        VALUES ('{a}',
        '{avg}',
        '{c_id}',
        {age},
        '{loc}')
        '''.format(
            a = account_history_string,
            avg = data['behavioral_patterns']['avg_transaction_value'],
            c_id = data['customer_id'],
            age = data['demographics']['age'],
            loc = data['demographics']['location']
        )
        # print(insert_query)
        cursor.execute(insert_query)
        # Commit the transaction
        connection.commit()
        # Close the cursor and connection
        cursor.close()
        connection.close()
        counter += 1
        print(counter)
        if counter == len(result) :
            break
else:
    print("Failed to fetch data from the API.")