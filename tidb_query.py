import mysql.connector

tidb_config = {
    'host': 'gateway01.ap-southeast-1.prod.aws.tidbcloud.com',
    'port': 4000,
    'user': 'Wo9LP4kv11iuK9y.root',
    'password': 'xIyUnY8XN7Iwtarh',
    'database': 'GUVI'
}

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

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS vino (name TEXT,address TEXT,id INTEGER) 
        """
    
    print(f'Create query: {create_table_query}')
    cursor.execute(create_table_query)

    conn.commit()
    print(f"Table name vino created")

except mysql.connector.Error as e:
    print(f"An error occurred while uploading data to TiDB: {e}")

finally:
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'conn' in locals() and conn:
        conn.close()