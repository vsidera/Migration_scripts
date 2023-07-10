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
                customers.mobile_number,
                replace(customers.custom_field ->> 'cf_fuels_used'::text, '\u200b'::text, ' '::text) AS fuel_used,
                replace(customers.custom_field ->> 'cf_primary_fuel'::text, '\u200b'::text, ' '::text) AS primary_fuel,
                replace(customers.custom_field ->> 'cf_meals_cooked_per_day'::text, '\u200b'::text, ' '::text) AS meals_no,
                replace(customers.custom_field ->> 'cf_any_improved_cook_stove'::text, '\u200b'::text, ' '::text) AS improved_cookstove,
                replace(customers.custom_field ->> 'cf_does_the_customer_own_a_burn_product'::text, '\u200b'::text, ' '::text) AS burn_products,
                replace(customers.custom_field ->> 'cf_household_size'::text, '\u200b'::text, ' '::text) AS household_size,
                replace(customers.custom_field ->> 'cf_education'::text, '\u200b'::text, ' '::text) AS education_level,
                replace(customers.custom_field ->> 'cf_occupation_status'::text, '\u200b'::text, ' '::text) AS employment_status,
                replace(customers.custom_field ->> 'cf_occupation'::text, '\u200b'::text, ' '::text) AS occupation,
                replace(customers.custom_field ->> 'cf_likelihood_to_purchase_a_product'::text, '\u200b'::text, ' '::text) AS likely_to_purchase
            FROM customers
            WHERE customers.mobile_number IS NOT NULL
            ORDER BY customers.created_at DESC
            OFFSET 0
            LIMIT 50;
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

def map_responses_to_contact(data):

    # import pdb; pdb.set_trace()
    try:
        # Connect to the target database
        target_conn = psycopg2.connect(target_db_connection_string)
        target_cursor = target_conn.cursor()

        # Create a list to store the rows with appended customer_id
        rows_with_customer_id = []

        # Iterate over the data and map survey responses to customers in the target database
        for row in data:
            print("ROW!!!!!!!!!!!", row)
            mobile_number = row[0]  # Accessing mobile_number from index 0

            # Fetch the customer ID from the target database based on the mobile number
            query = f"SELECT id FROM contacts WHERE phone = '{mobile_number}'"
            target_cursor.execute(query)
            result = target_cursor.fetchone()

            if result is not None:

                customer_id = result[0]
                
                # Create a new list to store the updated row
                updated_row = list(row)

                # Append the customer_id to the respective row
                updated_row.append(customer_id)

                # Use the updated_row instead of row
                row = tuple(updated_row)

                rows_with_customer_id.append(row)
  
            else:
                print(f"No customer found for mobile number: {mobile_number}")
            
        # Print the rows with customer_id appended
        for row in rows_with_customer_id:
            print("Updated Row:", row)    
            
            # Rest of the code...
        import pdb; pdb.set_trace()
        # Commit the changes to the target database
        target_conn.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the target database:", error)

    finally:
        if target_cursor:
            target_cursor.close()
        if target_conn:
            target_conn.close()




# Fetch data from the source database
data = fetch_data_from_source()

map_responses_to_contact(data)

# for contact in duplicate_contacts:
#     print(contact)
