from pyhive import hive

def execute_hive_query(query):
    with hive.connect(host='localhost', database='transactions_data') as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            return result

# Rule 1: Transactions with amounts higher than a certain threshold
amount_threshold = 1000
query_1 = f"""
    SELECT *
    FROM transactions_data.customers
    WHERE avg_transaction_value > {amount_threshold}
"""

result_1 = execute_hive_query(query_1)
print("Rule 1 Results:")
print(result_1)

# Rule 2: High frequency of transactions per customer
transaction_threshold = 5
query_2 = f"""
    SELECT customer_id, COUNT(*) AS transaction_count
    FROM transactions_data.customers
    GROUP BY customer_id
    HAVING transaction_count > {transaction_threshold}
"""

result_2 = execute_hive_query(query_2)
print("\nRule 2 Results:")
print(result_2)


