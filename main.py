import pandas as pd 
csv_file = r"C:\Users\moadh\OneDrive\Desktop\transaction_dataset.csv"

df = pd.read_csv(csv_file)
# Remove the "product_class" and "product_size" columns
columns_to_remove = ["product_class", "product_size"]
df = df.drop(columns=columns_to_remove)
# Rename the columns (replace with actual column names)
df = df.rename(columns={
    "t_id": "transaction_id",
    "p_id": "product_id",
    "c_id": "customer_id",
    "tr_date": "transaction_date",
    "tr_id" : "transaction_id"
   
})

# --- Inspect data ---
print(df)
# Exporting the cleaned DataFrame to a CSV file (excluding the index)
df.to_csv("cleaned_dataset.csv", index=False)

approved_df = df[df['order_status'] == 'Approved']
df

print(approved_df)

from datetime import datetime
# Converting the 'transaction_date' column to a datetime data type
df['transaction_date'] = pd.to_datetime(df['transaction_date'], format='%d-%m-%Y %H:%M')

# Extracting relevant date components
df['transaction_year'] = df['transaction_date'].dt.year
df['transaction_month'] = df['transaction_date'].dt.month
df['transaction_day'] = df['transaction_date'].dt.day


df['transaction_hour'] = df['transaction_date'].dt.hour
df['transaction_minute'] = df['transaction_date'].dt.minute
df_cleaned = df.dropna()
df

df.to_csv(r"C:\Users\moadh\OneDrive\Desktop\approved_dataset.csv", index=False)
import sqlite3
df = pd.read_csv(r"C:\Users\moadh\OneDrive\Desktop\approved_dataset.csv")
db_file = 'approved_dataset.db'
conn = sqlite3.connect(db_file)
create_table_query = "CREATE TABLE IF NOT EXISTS approved_data (transaction_id INT, product_id INT, customer_id INT,
transaction_date DATE, online_order BOOLEAN, order_status TEXT, 
brand TEXT, product_line TEXT, list_price REAL, standard_cost REAL,
product_first_sold_date REAL)"

df.to_sql('approved_data', conn, if_exists='replace', index=False)

conn.commit()
conn.close()
# Connect to the SQLite database
db_file = 'approved_dataset.db'  
conn = sqlite3.connect(db_file)

# Creating a cursor object
cursor = conn.cursor()

# Execute a SQL query to select all rows from the "approved_data" table
cursor.execute("SELECT * FROM approved_data")

# Fetching all the rows from the result set
table_contents = cursor.fetchall()

# Close the database connection
conn.close()

# Display the table contents
for row in table_contents:
    print(row)

%load_ext sql
%sql sqlite:///approved_dataset.db
%sql SELECT brand, standard_cost FROM approved_data;


%load_ext sql
%sql sqlite:///approved_dataset.db
#Identifying Products with a Profit Margin Greater Than 50%
query = """
SELECT product_id, brand, (list_price - standard_cost) AS profit_margin
FROM approved_data
WHERE (list_price - standard_cost) > (standard_cost * 0.5);
"""

result = %sql $query
result

%load_ext sql
%sql sqlite:///approved_dataset.db
#Retrieving the Top 10 Customers by Total Spending
query = """
SELECT customer_id, SUM(list_price) AS total_spending
FROM approved_data
GROUP BY customer_id
ORDER BY total_spending DESC
LIMIT 10;
"""

result = %sql $query
result

%load_ext sql
%sql sqlite:///approved_dataset.db
#Average List Price by Brand
query = """
SELECT brand, AVG(list_price) AS average_list_price
FROM approved_data
GROUP BY brand;

"""

result = %sql $query
result











