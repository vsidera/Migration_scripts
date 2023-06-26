import datetime
import psycopg2

# Source database connection string
source_db_connection_string = "postgres://salesapp:gx40E2t5WcBq@burn-ecore-production.cluster-cbxsbosv8vtr.eu-west-1.rds.amazonaws.com:5432/salesdata?sslmode=require"

# Target database connection string
target_db_connection_string = "postgres://salesapp:gx40E2t5WcBq@burn-ecore-production.cluster-cbxsbosv8vtr.eu-west-1.rds.amazonaws.com:5432/customerdb?sslmode=require"

def fetch_data_from_source():
    try:
        # Connect to the source database
        source_conn = psycopg2.connect(source_db_connection_string)
        source_cursor = source_conn.cursor()

        # Execute the query to fetch data from the source database
        query = """
            SELECT
                customers.id,
                customers.mobile_number,
                replace(customers.custom_field ->> 'cf_serial_number'::text, '\u200b'::text, ' '::text) AS serial_number

            FROM customers
            WHERE customers.country IN ('Kenya', 'Tanzania', 'Zambia')
            ORDER BY customers.created_at DESC
            OFFSET 0
            LIMIT 6000;
                """
        
        source_cursor.execute(query)

        # Fetch all the rows from the query result
        rows = source_cursor.fetchall()

        # Close the source database connection
        source_cursor.close()
        source_conn.close()

        return rows

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the source database:", error)    


def migrate_data_to_target(data):
    try:
        # Connect to the target database
        target_conn = psycopg2.connect(target_db_connection_string)
        target_cursor = target_conn.cursor()

        # Fetch contact IDs from the target database
        target_cursor.execute("SELECT id, phone FROM contacts;")
        contact_records = target_cursor.fetchall()

        # Create a phone-to-id mapping
        phone_to_id = {record[1]: record[0] for record in contact_records}

        # Update each row in the target database
        for row in data:
            phone_number = row[1]
            contact_id = phone_to_id.get(phone_number)
            if contact_id is not None:
                # Check if the serial number is empty or null
                serial_number = row[2]
                if not serial_number:
                    serial_number = None

                try:
                    # Update the serial_number field for the corresponding contact_id
                    target_cursor.execute(
                        "UPDATE accounts SET serial_number = %s WHERE contact_id = %s",
                        (serial_number, contact_id)
                    )
                    target_conn.commit()
                except psycopg2.Error as e:
                    print("Error updating serial number:", e)
                    target_conn.rollback()  # Rollback the transaction
            else:
                print(f"No matching contact found for phone number: {phone_number}")

        # Close the target database connection
        target_cursor.close()
        target_conn.close()

        print("Data migration completed successfully.")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the target database:", error)


# Fetch data from the source database
data = fetch_data_from_source()

# Insert data into the target database
migrate_data_to_target(data)