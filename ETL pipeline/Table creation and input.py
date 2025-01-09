import psycopg2
from psycopg2 import sql

# Connection details
conn = psycopg2.connect(
    host="localhost",
    dbname="postgres",
    user="postgres",
    password="123",
    port=5432
)

try:
    # Check if the connection is open
    if conn.closed:
        raise Exception("Database connection is already closed.")

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Create table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS retail_store (
        id SERIAL PRIMARY KEY,
        brand VARCHAR(50),
        item VARCHAR(100),
        barcode VARCHAR(20)
    );
    """
    cur.execute(create_table_query)

    # Insert random data into the table with repeated brands
    insert_data_query = """
    INSERT INTO retail_store (brand, item, barcode) VALUES 
        ('Pantene', 'Shampoo', '123456789012'),
        ('Pantene', 'Conditioner', '987654321098'),
        ('Dove', 'Soap', '456789123456'),
        ('Dove', 'Bodywash', '321654987321'),
        ('Nivea', 'Lotion', '789123456789'),
        ('Nivea', 'Face Cream', '123987654321'),
        ('Colgate', 'Toothpaste', '987321654098'),
        ('Colgate', 'Mouthwash', '654123987654'),
        ('Listerine', 'Mouthwash', '321987654321'),
        ('Listerine', 'Breath Strips', '789456123789'),
        ('Gillette', 'Shaving Cream', '123654789012'),
        ('Gillette', 'Razor', '456321987654'),
        ('Revlon', 'Lipstick', '789123456012'),
        ('Revlon', 'Mascara', '321456987123'),
        ('Maybelline', 'Eye Liner', '654789321456'),
        ('Maybelline', 'Foundation', '987654123789'),
        ('Oreal', 'Shampoo', '123987654789'),
        ('Oreal', 'Conditioner', '456789321012'),
        ('Olay', 'Face Cream', '789654123321'),
        ('Olay', 'Body Lotion', '321987654789'),
        ('Chanel', 'Perfume', '654123987012'),
        ('Chanel', 'Lipstick', '987321654789'),
        ('MAC', 'Foundation', '123456987123'),
        ('MAC', 'Highlighter', '456123789654'),
        ('Lancome', 'Mascara', '789987654321'),
        ('Lancome', 'Blush', '321654987654'),
        ('Estee Lauder', 'Concealer', '654987321789'),
        ('Estee Lauder', 'Lipstick', '987123456789'),
        ('Kerastase', 'Hair Serum', '123321654987'),
        ('Kerastase', 'Shampoo', '456987123654'),
        ('Old Spice', 'Deodorant', '789321654987'),
        ('Old Spice', 'Body Wash', '321123456789'),
        ('Dove', 'Body Lotion', '654789123321'),
        ('Dove', 'Hand Cream', '987654321123'),
        ('Vaseline', 'Lip Balm', '123987456321'),
        ('Vaseline', 'Hand Lotion', '456123789321'),
        ('Nivea', 'Sunscreen', '789654123456'),
        ('Nivea', 'Face Wash', '321987654012'),
        ('Revlon', 'Eye Shadow', '654123987789'),
        ('Revlon', 'Nail Polish', '987321654654'),
        ('Pantene', 'Hair Spray', '123654987321'),
        ('Pantene', 'Hair Mask', '456789123789'),
        ('Colgate', 'Toothbrush', '789321456987'),
        ('Colgate', 'Dental Floss', '321654789012'),
        ('Gillette', 'Aftershave', '654987321456'),
        ('Gillette', 'Shaving Gel', '987123654321'),
        ('MAC', 'Compact Powder', '123456789987'),
        ('MAC', 'Makeup Remover', '456321789123'),
        ('Dove', 'Shampoo', '789654123789');
    """
    cur.execute(insert_data_query)

    # Commit the changes
    conn.commit()


except Exception as e:
    print("An error occurred:", e)

finally:
    # Close the cursor and connection
    if 'cur' in locals() and not cur.closed:
        cur.close()
    if conn and not conn.closed:
        conn.close()
