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
                customers.firstname,
                customers.lastname,
                customers.mobile_number,
                customers.country,
                replace(customers.custom_field ->> 'cf_agent_id'::text, '\u200b'::text, ' '::text) AS agent_id,
                replace(customers.custom_field ->> 'cf_region'::text, '\u200b'::text, ' '::text) AS region,
                replace(customers.custom_field ->> 'cf_sub_region'::text, '\u200b'::text, ' '::text) AS sub_region,
                replace(customers.custom_field ->> 'cf_gender'::text, '\u200b'::text, ' '::text) AS gender,
                replace(customers.custom_field ->> 'cf_pos_longitude'::text, '\u200b'::text, ' '::text) AS longitude,
                replace(customers.custom_field ->> 'cf_pos_latitude'::text, '\u200b'::text, ' '::text) AS latitude,
                replace(customers.custom_field ->> 'cf_customer_status'::text, '\u200b'::text, ' '::text) AS status
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
print("Contacts with duplicate key violations:")
for contact in duplicate_contacts:
    print(contact)
