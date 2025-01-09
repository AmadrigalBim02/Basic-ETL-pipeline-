# Basic-ETL-pipeline-
Why use a ETL pipeline? 
 
Extract
- Purpose: Pull data from the `retail_store` table in the PostgreSQL database.
- Implementation:
  - A connection to the PostgreSQL database is established using `psycopg2`.
  - A SQL query (`SELECT * FROM retail_store;`) retrieves all rows from the table.
  - The fetched data is converted into a pandas DataFrame, making it easier to manipulate the data for the next stage.

---
 Transform
- Purpose: Perform transformations on the extracted data to prepare it for the target system.
- Implementation:
  - A new column, `category`, is added to the DataFrame. This column categorizes items based on the `item` field:
    - If `item` contains "Shampoo" or "Conditioner," the `category` is labeled as "Personal Care."
    - Otherwise, it is labeled as "Others."
  - This is done using the `apply` function in pandas with a conditional lambda function.

---

 Load
- Purpose: Insert the transformed data into a new table (`retail_store_transformed`) in the PostgreSQL database.
- Implementation:
  - A new table `retail_store_transformed` is created if it doesn’t already exist. The table has the following columns: `id`, `brand`, `item`, `barcode`, and `category`.
  - Each row of the transformed DataFrame is inserted into this table using SQL `INSERT` statements.
  - The `psycopg2` library handles database operations, and the data is committed once all rows are inserted.

---

 ETL Pipeline Workflow
1. Extracting Data:
   - The `extract_data()` function retrieves data from the source (`retail_store` table).
   - The data is displayed using `.head()` for verification.
   
2. Transforming Data:
   - The `transform_data()` function applies transformations, like adding the `category` column.
   - Transformed data is displayed for verification.
   
3. Loading Data:
   - The `load_data()` function writes the transformed data into the `retail_store_transformed` table.
   - It ensures data integrity by committing changes only after all rows are successfully inserted.

---

 Benefits of This ETL Pipeline
- Scalability: You can add more complex transformations or additional steps (e.g., cleaning) in the `transform_data()` function.
- Modularity: Each stage (Extract, Transform, Load) is encapsulated in its own function, making the code easy to maintain and expand.
- Reusability: The pipeline can be reused for other tables or datasets by adjusting the queries and transformation logic.
