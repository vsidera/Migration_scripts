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
            LIMIT 500;
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
    # import pdb; pdb.set_trace()
    try:
        # Connect to the target database
        target_conn = psycopg2.connect(target_db_connection_string)
        target_cursor = target_conn.cursor()

        # Fetch contact IDs from the target database
        target_cursor.execute("SELECT id, phone FROM contacts;")
        contact_records = target_cursor.fetchall()

        # Create a phone-to-id mapping
        phone_to_id = {record[1]: record[0] for record in contact_records}

        # Prepare the INSERT statement for the target database
        insert_query = """
            INSERT INTO accounts (
                serial_number
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s
            );
        """

        # Insert each row into the target database
        for row in data:
            row_list = list(row)

            phone_number = row[1]
            contact_id = phone_to_id.get(phone_number)
            if contact_id is not None:
                row_list[0] = contact_id  # Replace the contact_id with the actual ID
            else:
                print(f"No matching contact found for phone number: {phone_number}")
                continue

            product = row[3]  # Get the product value from the row
            converted_product = convert_product(product)  # Convert the product
            if converted_product is not None:
                row_list[1] = converted_product  # Replace the product with the converted value
            else:
                print(f"Unknown product: {product}")
                continue

            payplan = row[4]  # Get the payplan value from the row
            if payplan is not None:
                converted_payplan = int(payplan)  # Convert the payplan to an integer
                row_list[3] = converted_payplan  # Replace the payplan with the converted value
            else:
                row_list[3] = 100

            status = row[7]
            if status is not None:
                capitalised_status = status.upper()
            else:
                capitalised_status = 'No Status'
            row_list[5] = capitalised_status

            delivery_date = row[6]
            if delivery_date == "Now":
                delivery_date = row[8]
            else:
                if delivery_date is None:
                    delivery_date = datetime.datetime.now()  # Set delivery date to current timestamp
                else:
                    try:
                        if isinstance(delivery_date, str):
                            delivery_date = datetime.datetime.strptime(delivery_date, "%Y-%m-%d %H:%M:%S")  # Parse the delivery date
                    except ValueError:
                        delivery_date = datetime.datetime.now()  # Set delivery date to current timestamp
            if isinstance(delivery_date, datetime.datetime):
                row_list[4] = delivery_date.strftime("%Y-%m-%d %H:%M:%S")


            if row[2] == "Kenya":
                external_id = row[5]
            elif row[2] == "Tanzania":
                external_id = row[0]
            else:
                external_id = row[5]

            row_list[2] = external_id

            try:
                target_cursor.execute(insert_query, (
                    row_list[0],
                    row_list[1],
                    row_list[2],
                    row_list[3],
                    row_list[4],
                    row_list[5],
                    row_list[9]
                ))
                target_conn.commit()
            except psycopg2.IntegrityError as e:
                print("Skipping account creation due to duplicate key violation:", e)
                target_conn.rollback()  # Rollback the transaction

        # Commit the transaction
        target_conn.commit()

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
