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
                customers.country,
                replace(customers.custom_field ->> 'cf_product'::text, '\u200b'::text, ' '::text) AS product,
                replace(customers.custom_field ->> 'cf_payplan_id'::text, '\u200b'::text, ' '::text) AS payplan,
                replace(customers.custom_field ->> 'cf_national_id'::text, '\u200b'::text, ' '::text) AS national_id,
                replace(customers.custom_field ->> 'cf_delivery_schedule'::text, '\u200b'::text, ' '::text) AS delivery_date,
                replace(customers.custom_field ->> 'cf_customer_status'::text, '\u200b'::text, ' '::text) AS status,
                customers.created_at

            FROM customers
            WHERE customers.country IN ('Kenya', 'Tanzania', 'Zambia')
            ORDER BY customers.created_at DESC
            LIMIT 2;
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

def convert_product(product):
    if product == 'Jikokoa Classic':
        return 1
    elif product == 'Jikokoa Extra':
        return 2
    elif product == 'Jikokoa Pro':
        return 3
    elif product == 'Ecoa Wood':
        return 4
    elif product == 'Induction Cooker':
        return 5
    else:
        return None        


def migrate_data_to_target(data):

    import pdb; pdb.set_trace()
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
                contact_id,
                product_id,
                external_id,
                payplan,
                date,
                status
            )
            VALUES (
                %s, %s, %s, %s, %s, %s
            );
        """

        # Insert each row into the target database
        for row in data:
            phone_number = row[1]
            contact_id = phone_to_id.get(phone_number)
            if contact_id is not None:
                row[0] = contact_id  # Replace the contact_id with the actual ID
            else:
                print(f"No matching contact found for phone number: {phone_number}")
            
            product = row[3]  # Get the product value from the row
            converted_product = convert_product(product)  # Convert the product
            
            if converted_product is not None:
                row[3] = converted_product  # Replace the product with the converted value
            else:
                print(f"Unknown product: {product}")

            payplan = row[4]  # Get the payplan value from the row
            converted_payplan = int(payplan)  # Convert the payplan to an integer
            row[4] = converted_payplan  # Replace the payplan with the converted value

            status = row[7]
            capitalised_status = status.upper()
            row[7] = capitalised_status

            delivery_date = row[6]
            if delivery_date == "Now":
                delivery_date = row[8]
            else:
                delivery_date = row[6]
            row[6] = delivery_date

            if row[2] == "Kenya":
                external_id = row[5]
            elif row[2] == "Tanzania":
                external_id = row[0]
            else:
                external_id = row[5]

            row[2] = external_id  
            
            target_cursor.execute(insert_query, row)

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
