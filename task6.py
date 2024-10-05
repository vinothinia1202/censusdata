import os
import pandas as pd
from pymongo import MongoClient
import mysql.connector
import re

mongo_url = 'mongodb+srv://Vinothinia:Vinothiniatlas@cluster0.cdnxy.mongodb.net/'

tidb_config = {
    'host': 'gateway01.ap-southeast-1.prod.aws.tidbcloud.com',
    'port': 4000,
    'user': 'Wo9LP4kv11iuK9y.root',
    'password': 'xIyUnY8XN7Iwtarh',
    'database': 'GUVI'
}

csv_file = 'census_2011.csv'

# def upload_to_tidb_from_mongo(mongo_url):
# Derive the table name from the CSV file name without extension
table_name = os.path.splitext(os.path.basename(csv_file))[0]
print(f'Table name : {table_name}')

# Fetch data from MongoDB
# Fetch data from MongoDB
try:
    client = MongoClient(mongo_url)
    db = client['newdb']
    census_collection = db['census']
    data = list(census_collection.find({}, {"_id": 0}))  # Exclude MongoDB's internal '_id' field
    df = pd.DataFrame(data)
    df = df.drop(columns="District_code", errors='ignore')  # Drop 'District_code' if it exists
except Exception as e:
    print(f"An error occurred while fetching data from MongoDB Atlas: {e}")
    exit(1)

# Check for duplicate columns and handle them
print("Original columns:")
# print(df.columns.tolist())

# Identify duplicate columns and make them unique
duplicate_columns = df.columns[df.columns.duplicated()].unique()
# print(f"Duplicate columns found: {duplicate_columns}")

for col in duplicate_columns:
    df[col] = df.groupby(col).cumcount()  # Add a suffix to duplicates
    df.rename(columns={col: f"{col}_{df[col].astype(str)}"}, inplace=True)

# Rename long column names if needed (limit to 64 characters)
long_columns = [col for col in df.columns if len(col) > 64]
for col in long_columns:
    new_col = re.sub(r'[^A-Za-z0-9_]', '_', col)[:64]  # Create a shorter name, preserving alphanumeric and underscores
    df.rename(columns={col: new_col}, inplace=True)
    print(f"Renamed column '{col}' to '{new_col}'")

# Replace NaN values with None for MySQL compatibility
df = df.where(pd.notnull(df), None)

# Save the transformed DataFrame to a CSV file (for debugging purposes)
df.to_csv("check_after_renameColumn.csv", index=False)

# Connect to TiDB using mysql.connector
try:
    print(f'Entered try block')
    conn = mysql.connector.connect(
        host=tidb_config['host'],
        port=tidb_config['port'],
        user=tidb_config['user'],
        password=tidb_config['password'],
        database=tidb_config['database']
    )
    cursor = conn.cursor()
    print(f"Connection established.")

    # Create the table in TiDB with appropriate column types
    column_definitions = []
    for col in df.columns:
        column_definitions.append(f"`{col}` VARCHAR(100)")

    # Create the SQL table
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}_2` (
            {', '.join(column_definitions)}
        );
    """
    print(f'Create query: {create_table_query}')
    cursor.execute(create_table_query)
    print("Table creation query executed successfully.")
    

    print(f"Final column name : {df.columns.to_list()}")
    # Prepare for data insertion
    insert_query = f"INSERT INTO `{table_name}_2` ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({', '.join(['%s' for _ in df.columns])})"

    # Debug: Print the insert query and the first row of data to be inserted
    print("Insert Query:")
    print(insert_query)
    print("First row of data to insert:")
    print(df.values.tolist()[0])
    df.fillna('', inplace=True)
    # Insert data into the table in smaller batches
    batch_size = 500  # Define the batch size
    data_to_insert = df.values.tolist()

    for i in range(0, len(data_to_insert), batch_size):
        batch = data_to_insert[i:i + batch_size]
        cursor.executemany(insert_query, batch)
        conn.commit()  # Commit after each batch
        print(f"Inserted batch {i // batch_size + 1} of {len(data_to_insert) // batch_size + 1}")

    print(f"Data uploaded to TiDB, table name: {table_name}.")

except mysql.connector.Error as e:
    print(f"An error occurred while uploading data to TiDB: {e}")

finally:
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'conn' in locals() and conn:
        conn.close()