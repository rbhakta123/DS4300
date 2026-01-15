import pandas as pd
import mysql.connector
from mysql.connector import Error

# 1. Read the CSV file
csv_file = 'hw1_data/follows.csv'
df = pd.read_csv(csv_file)

# 2. Connect to MySQL
try:
    connection = mysql.connector.connect(
        host='localhost',        # Replace with your host
        user='ruhan',    # Replace with your username
        password='abc123',# Replace with your password
        database='twitter' # Replace with your database
    )

    if connection.is_connected():
        cursor = connection.cursor()

        # 3. Prepare the INSERT statement
        insert_query = """
        INSERT INTO follows (follower_id, followee_id)
        VALUES (%s, %s)
        """

        # 4. Iterate over the DataFrame and insert rows
        data_to_insert = list(df.itertuples(index=False, name=None))
        cursor.executemany(insert_query, data_to_insert)

        # 5. Commit changes
        connection.commit()
        print(f"{cursor.rowcount} rows were inserted successfully.")

except Error as e:
    print("Error while connecting to MySQL", e)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
