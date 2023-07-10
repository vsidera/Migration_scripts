import psycopg2
from datetime import datetime, timedelta
import math
import decimal
import requests
import openpyxl

# crm database connection string
crm_db_connection_string = "postgres://salesapp:gx40E2t5WcBq@burn-ecore-production.cluster-cbxsbosv8vtr.eu-west-1.rds.amazonaws.com:5432/customerdb?sslmode=require"

fineract_db_connection_string = "postgresql://postgres:9Kqy&sp8&D!i@burn-ecore-production.cluster-cbxsbosv8vtr.eu-west-1.rds.amazonaws.com:5432/fineract_default"

def fetch_accounts_from_crm():
    try:
        # Connect to the CRM database
        source_conn = psycopg2.connect(crm_db_connection_string)
        source_cursor = source_conn.cursor()

        # Execute the query to fetch data from the source database
        query = """
            SELECT
                id,
                external_id,
                serial_number,
                country_id,
                phone,
                first_name,
                last_name
            FROM contact_accounts_product_payplan_v
            WHERE serial_number IS NOT NULL AND serial_number != ' ' AND payplan='131'
            ORDER BY created_at DESC
            OFFSET 254
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


def fetch_figures_from_fineract(data):

    try:
        # Connect to the fineract database
        target_conn = psycopg2.connect(fineract_db_connection_string)
        target_cursor = target_conn.cursor()

        modified_data = []

        for row in data:
            # Extract the external_id from the row
            external_id = row[1]
            # Execute a query to fetch the desired data from the target database
            query = """
                SELECT
                    total_overdue_derived
                FROM mloan_product_view
                WHERE external_id = %s AND loan_status_id = 300;
            """
            target_cursor.execute(query, (external_id,))

            # Fetch the row from the query result
            target_row = target_cursor.fetchone()

            if target_row:
                # Append the data from the target database to the original row
                new_row = row + target_row

                modified_data.append(new_row)

        # Close the target database connection
        target_cursor.close()
        target_conn.close()
        
        return modified_data 

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to the target database:", error)        


def get_excel(results):

    import pdb; pdb.set_trace()
    # Create a new Excel workbook
    workbook = openpyxl.Workbook()

    # Select the active worksheet
    worksheet = workbook.active

    # Write the headers
    headers = [
        'Phone', 'Account No', 'Serial No',
        'Country Code', 'First Name', 'Last Name', 'Arrears'
    ]
    worksheet.append(headers)

    # Write the data
    for result in results:
        row = [
            result['phone'],
            result['account_no'],
            result['serial_no'],
            result['country_code'],
            result['first_name'],
            result['last_name'],
            result['arrears']
        ]
        worksheet.append(row)

    workbook.save('arrears.xlsx')

def send_expiry_sms_early(sms_data):
    
    phone = sms_data.get('phone')
    country_code = sms_data.get('country_code')
    first_name = sms_data.get('first_name')
    last_name = sms_data.get('last_name')
    account_no = sms_data.get('account_no')
    arrears = sms_data.get('arrears') or 0

    arrears = round(arrears, 2)

    text_message = f"Dear {first_name} {last_name}.Thank you for making your payments on time. Going forward, all delayed payments will lead to locking of your cooker. For queries: 0768 473017 or 0113 944491"

    payload = {
        "customer_id": "12345",
        "phone_number": phone,
        "text_message": text_message,
        "callback_url": "",
        "country_code": country_code,
        "channel": "sms",
        "sender_name": "Ecoa"
    }

    url = "https://yz3bcznv7k.us-east-1.awsapprunner.com/api/message/send"

    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("SMS sent successfully.")
        else:
            print("Failed to send SMS. Status code:", response.status_code)
    except requests.exceptions.RequestException as error:
        print("Error sending SMS:", error)


data = fetch_accounts_from_crm()

modified_data = fetch_figures_from_fineract(data)

get_excel(modified_data)



