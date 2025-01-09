import psycopg2
import pandas as pd

def extract_data():
    """Extract data from the retail_store table."""
    conn = psycopg2.connect(
        host="localhost",
        dbname="postgres",
        user="postgres",
        password="123",
        port=5432
    )
    try:
        cur = conn.cursor()
        query = "SELECT * FROM retail_store;"
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        data = pd.DataFrame(rows, columns=columns)
        return data
    except Exception as e:
        print("An error occurred during extraction:", e)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def transform_data(data):
    """Transform the extracted data."""
    # Example Transformation: Add a new column 'category' based on the item name
    data['category'] = data['item'].apply(lambda x: 'Personal Care' if 'Shampoo' in x or 'Conditioner' in x else 'Others')
    return data

def load_data(data):
    """Load transformed data into a new table."""
    conn = psycopg2.connect(
        host="localhost",
        dbname="postgres",
        user="postgres",
        password="123",
        port=5432
    )
    try:
        cur = conn.cursor()
        # Create a new table for transformed data
        create_table_query = """
        CREATE TABLE IF NOT EXISTS retail_store_transformed (
            id SERIAL PRIMARY KEY,
            brand VARCHAR(50),
            item VARCHAR(100),
            barcode VARCHAR(20),
            category VARCHAR(50)
        );
        """
        cur.execute(create_table_query)

        # Insert transformed data
        for _, row in data.iterrows():
            insert_query = """
            INSERT INTO retail_store_transformed (brand, item, barcode, category)
            VALUES (%s, %s, %s, %s);
            """
            cur.execute(insert_query, (row['brand'], row['item'], row['barcode'], row['category']))

        conn.commit()
    except Exception as e:
        print("An error occurred during loading:", e)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def etl_pipeline():
    """Execute the ETL pipeline."""
    print("Extracting data...")
    data = extract_data()
    print("Data extracted:")
    print(data.head())

    print("Transforming data...")
    transformed_data = transform_data(data)
    print("Data transformed:")
    print(transformed_data.head())

    print("Loading data...")
    load_data(transformed_data)
    print("Data loaded successfully.")

if __name__ == "__main__":
    etl_pipeline()
