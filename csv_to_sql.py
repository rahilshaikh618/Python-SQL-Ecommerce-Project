import pymysql
import os
import pandas as pd

# Database connection
conn = pymysql.connect(
    host="localhost",
    user="root",
    password="your password",
    database="ecommerce",
    local_infile=1
)
cursor = conn.cursor()

folder_path = r'add your path'
csv_files = [
    ('customers.csv', 'customer'),
    ('orders.csv', 'orders'),
     ('order_items.csv', 'orders_items'),
    ('sellers.csv', 'sellers'),
    ('products.csv', 'products'),
    ('geolocation.csv', 'geolocation'),
    ('payments.csv', 'payments')
]

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file).replace('\\', '/')
    print(f"\n➡ Importing '{csv_file}' into `{table_name}`...")

    # Step 1: Read first row of CSV to get column names
    df = pd.read_csv(file_path, nrows=0)
    columns = df.columns
    columns_clean = [col.strip().replace(' ', '_').replace('-', '_').replace('.', '_') for col in columns]

    # Step 2: Create table if it doesn't exist
    col_definitions = ", ".join([f"`{col}` TEXT" for col in columns_clean])
    create_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({col_definitions});"
    cursor.execute(create_query)
    conn.commit()

    # Step 3: Import CSV using LOAD DATA LOCAL INFILE
    try:
        query = f"""
        LOAD DATA LOCAL INFILE '{file_path}'
        INTO TABLE `{table_name}`
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"' 
        LINES TERMINATED BY '\\n'
        IGNORE 1 ROWS;
        """
        cursor.execute(query)
        conn.commit()
        print(f"✅ Imported {csv_file} successfully")
    except Exception as e:
        print(f"❌ Error importing {csv_file}: {e}")

cursor.close()
conn.close()
print("\n🎉 All CSVs imported successfully!")
