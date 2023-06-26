import psycopg2

source_db_connection_string = "postgresql://postgres:9Kqy&sp8&D!i@burn-ecore-production.cluster-cbxsbosv8vtr.eu-west-1.rds.amazonaws.com:5432/fineract_default"

# Target database connection string
target_db_connection_string = "postgres://salesapp:gx40E2t5WcBq@burn-ecore-production.cluster-cbxsbosv8vtr.eu-west-1.rds.amazonaws.com:5432/customerdb?sslmode=require"

try:
    # Connect to the source database
    source_conn = psycopg2.connect(source_db_connection_string)
    source_cursor = source_conn.cursor()

    # Connect to the target database
    target_conn = psycopg2.connect(target_db_connection_string)
    target_cursor = target_conn.cursor()

    

    # Execute the query to update records in the target database
    source_cursor.execute("SELECT external_id, product_id FROM m_loan")
    # import pdb; pdb.set_trace()
    for row in source_cursor:
        external_id, product_id = row

        # Check if the corresponding record exists in the target database
        target_cursor.execute("SELECT external_id FROM accounts WHERE external_id = %s AND payplan = '100'", (external_id,))
        if target_cursor.fetchone():
            # Update the payplan value with the product_id from the source database
            target_cursor.execute("UPDATE accounts SET payplan = %s WHERE external_id = %s AND payplan = '100'", (product_id, external_id))

    # Commit the changes to the target database
    target_conn.commit()

    print("Records updated successfully.")
except psycopg2.Error as error:
    print("Error while connecting to the database:", error)
finally:
    # Close the cursors and connections
    if source_cursor:
        source_cursor.close()
    if target_cursor:
        target_cursor.close()
    if source_conn:
        source_conn.close()
    if target_conn:
        target_conn.close()
