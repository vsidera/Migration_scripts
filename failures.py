import psycopg2
from openpyxl import Workbook

# Source database connection string
source_db_connection_string = "postgres://salesapp:gx40E2t5WcBq@burn-ecore-production.cluster-cbxsbosv8vtr.eu-west-1.rds.amazonaws.com:5432/salesdata?sslmode=require"

# Target database connection string
target_db_connection_string = "postgres://salesapp:gx40E2t5WcBq@burn-ecore-production.cluster-cbxsbosv8vtr.eu-west-1.rds.amazonaws.com:5432/customerdb?sslmode=require"

# Column names for phone number in source and target databases
source_phone_column = "mobile_number"
target_phone_column = "phone"
excluded_column = "custom_field"

# Establish connections to source and target databases
source_conn = psycopg2.connect(source_db_connection_string)
target_conn = psycopg2.connect(target_db_connection_string)

# Create cursors for executing queries
source_cursor = source_conn.cursor()
target_cursor = target_conn.cursor()

# Retrieve data from target database
target_cursor.execute(f"SELECT {target_phone_column} FROM contacts")
target_phone_numbers = set(record[0] for record in target_cursor.fetchall())


# Retrieve column names and records from source database
source_cursor.execute(f"SELECT * FROM customers")
source_column_names = [desc[0] for desc in source_cursor.description]
source_records = source_cursor.fetchall()

# Get the index of the excluded column
excluded_column_index = source_column_names.index(excluded_column)

# Filter source records based on missing phone numbers
missing_records = [record for record in source_records if record[source_column_names.index(source_phone_column)] not in target_phone_numbers]

# Create an Excel workbook and sheet
workbook = Workbook()
sheet = workbook.active

# Write the headers to the Excel sheet (excluding the excluded column)
header_row = [column_name for column_name in source_column_names if column_name != excluded_column]
sheet.append(header_row)

# Write the missing records to the Excel sheet (excluding the excluded column)
for record in missing_records:
    filtered_record = [value for index, value in enumerate(record) if index != excluded_column_index]
    sheet.append(filtered_record)

# Save the workbook to a file
workbook.save("missing_records.xlsx")

# Close connections
source_cursor.close()
target_cursor.close()
source_conn.close()
target_conn.close()
