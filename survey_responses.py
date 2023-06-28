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
    # import pdb; pdb.set_trace()
    try:
        # Connect to the target database
        target_conn = psycopg2.connect(target_db_connection_string)
        target_cursor = target_conn.cursor()

        # List to store contacts with unique constraint violations
        duplicate_contacts = []

        # Iterate over the rows and insert or update data in the target database
        for row in data:
            country = row[3]  # country name
            country_id = None  # default value

            if country == 'Kenya':
                country_id = 1
            elif country == 'Tanzania':
                country_id = 2
            elif country == 'Zambia':
                country_id = 35

            gender = row[7]
            gender = gender.upper() if gender else "MALE"

            status = row[10]
            if status == 'Customer':
                capitalised_status = 'CUSTOMER'
            else:
                capitalised_status = 'ELIGIBLE'

            query = """
                INSERT INTO contacts (first_name, last_name, phone, country_id, salesperson_id, region, sub_region, gender, pos_longitude, pos_latitude, occupation, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (phone) DO UPDATE
                SET status = excluded.status;
            """
            values = (
                row[0],  # firstname
                row[1],  # lastname
                row[2],  # mobile_number
                country_id,  # country
                row[4],  # agent_id
                row[5],  # region
                row[6],  # sub_region
                gender,  # gender
                row[8],  # longitude
                row[9],  # latitude
                "Other",  # occupation
                capitalised_status
            )

            try:
                target_cursor.execute(query, values)
                target_conn.commit()
            except psycopg2.IntegrityError as e:
                target_conn.rollback()
                duplicate_contacts.append(row)
                print("Duplicate contact:", row)
        
        # Close the target database connection
        target_cursor.close()
        target_conn.close()

        print("Data migration completed successfully.")

        # Return the list of duplicate contacts
        return duplicate_contacts

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the target database:", error)

# Fetch data from the source database
data = fetch_data_from_source()

# Migrate the data to the target database
duplicate_contacts = migrate_data_to_target(data)


# Print the list of contacts with duplicate key violations
# print("Contacts with duplicate key violations:")
# for contact in duplicate_contacts:
#     print(contact)
