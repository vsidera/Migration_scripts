import psycopg2
from datetime import datetime, timedelta
import math
import decimal
import requests

# crm database connection string
crm_db_connection_string = "postgres://salesapp:gx40E2t5WcBq@burn-ecore-production.cluster-cbxsbosv8vtr.eu-west-1.rds.amazonaws.com:5432/customerdb?sslmode=require"

fineract_db_connection_string = "postgresql://postgres:9Kqy&sp8&D!i@burn-ecore-production.cluster-cbxsbosv8vtr.eu-west-1.rds.amazonaws.com:5432/fineract_default"

paygo_db_connection_string = "postgresql://postgres:NlQA1fKgUGZGhPYqiCFx@auth-manager.cpnlo4orrspa.eu-west-1.rds.amazonaws.com:5432/paygo"

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
            FROM contact_accounts_product_v
            WHERE serial_number IS NOT NULL AND serial_number <> ''
            ORDER BY created_at DESC
            OFFSET 0
            LIMIT 10;
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
                    disbursedon_date,
                    repay_every,
                    expected_maturedon_date,
                    fee_charges_charged_derived,
                    principal_amount,
                    total_repayment_derived,
                    instalment_amount_in_multiples_of,
                    total_overdue_derived
                FROM mloan_product_view
                WHERE external_id = %s;
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

def calculate_expiry_wallet(modified_data):
    
    results = []

    for row in modified_data:

        account_id = row[0]
        external_id = row[1]
        serial_number = row[2]
        country_id = row[3]
        phone = row[4]
        first_name = row[5]
        last_name = row[6]
        disbursed_on_date = row[7]
        repay_every = row[8]
        expected_matured_on_date = row[9]
        deposit = row[10]
        principal = row[11]
        total_paid = row[12]
        instalment = row[13]
        arrears = row[14]

        if country_id == 1:
            country_code = "+254"
        elif country_id == 2:
            country_code = "+255"
        else:
            country_code = "+260"
        
        default_expiry_date = '2022-07-10'

        today = datetime.now().date()
        # date_disbursed = datetime.strptime(disbursed_on_date, '%Y-%m-%d').date()
        date_disbursed = disbursed_on_date
        days_since_deposit = today - date_disbursed

        # maturity_date = datetime.strptime(expected_matured_on_date, '%Y-%m-%d').date()
        maturity_date = expected_matured_on_date
        total_loan_period = maturity_date - date_disbursed

        amount_paid = total_paid - deposit
        amt_to_be_on_track = (days_since_deposit/total_loan_period) * float(principal)

        deficit = float(amount_paid)-amt_to_be_on_track

        daily_rate = instalment/(repay_every*7)
        daily_rate = round(daily_rate, 2)

        if deficit <= 0:

            expiry_date = datetime.strptime(default_expiry_date, "%Y-%m-%d")
            expiry_date = expiry_date.isoformat()
            balance = 0
        else :
            expiry_date = (datetime.strptime(default_expiry_date, '%Y-%m-%d') + timedelta(days=int(math.floor(amount_paid - decimal.Decimal(amt_to_be_on_track)) / daily_rate))).date()
            expiry_date = expiry_date.isoformat() + 'T00:00:00'
            
            amount_paid = float(amount_paid)

            daily_rate = float(daily_rate)

            balance = (amount_paid - amt_to_be_on_track) % daily_rate
            balance = round(balance, 0)

        record = {
            'phone': phone,
            'expiry_date': expiry_date,
            'balance': balance,
            'account_no' : external_id,
            'serial_no' : serial_number,
            'country_code': country_code,
            'daily_rate': daily_rate,
            'first_name': first_name,
            'last_name': last_name,
            'arrears': arrears
        }
        results.append(record)
    return results

def post_to_paygo_db(paygo_data):

    try:
        for data in paygo_data:
            account_no = data['account_no']
            expiry_date = data['expiry_date']
            balance = data['balance']
            serial_no = data['serial_no']
            phone = data['phone']
            country_code = data['country_code']
            daily_rate = data['daily_rate']
            first_name = data['first_name']
            last_name = data['last_name']
            arrears = data['arrears']

            sms_data = {
                "country_code": country_code,
                "phone": phone,
                "daily_rate": daily_rate,
                "account_no": account_no,
                "first_name": first_name,
                "last_name": last_name,
                "expiry_date": expiry_date,
                "arrears": arrears
            }

            url_device = "https://itpnyugcfz.eu-west-1.awsapprunner.com/api/device"

            url_wallet = "https://itpnyugcfz.eu-west-1.awsapprunner.com/api/wallet"

            try:
                payload = {
                        "serialNo": serial_no,
                        "dailyRate": str(daily_rate),
                        "expiryDate": expiry_date
                      }
                # import pdb; pdb.set_trace()
                response = requests.post(url_device, json=payload)
                
                if response.status_code == 200:
                    device_id = response.json()["id"]
                    print(f"Device created successfully! Device ID: {device_id}")
                    payload = {
                        "deviceId": device_id,
                        "accountNo": account_no,
                        "balance": str(balance)
                    }
                    response = requests.post(url_wallet, json=payload)

                    if response.status_code == 200:
                        print(f"Wallet created successfully for DEVICE: {device_id}")
                        send_expiry_sms(sms_data)
                    else:
                        print(f"FAILED to create wallet for DEVICE: {device_id}")   

                else:
                    print("Failed to create device", response.status_code)
            except requests.exceptions.RequestException as error:
                print("Error creating device", error)

    except (Exception, psycopg2.Error) as error:
        print("Error while posting to the paygo db", error)

def send_expiry_sms(sms_data):
    
    phone = sms_data.get('phone')
    daily_rate = sms_data.get('daily_rate')
    country_code = sms_data.get('country_code')
    first_name = sms_data.get('first_name')
    last_name = sms_data.get('last_name')
    expiry_date = sms_data.get('expiry_date')
    account_no = sms_data.get('account_no')
    arrears = sms_data.get('total_overdue_derived') or 0

    text_message = f"Dear {first_name}{last_name}, your loan of {arrears} is due on {expiry_date} & your cooker will be locked. To avoid being locked, pay as little as {daily_rate} daily to M-PESA: Paybill 313348 Acc. No: {account_no}."

    payload = {
        "customer_id": "12345",
        "phone_number": "0711438911",
        "text_message": text_message,
        "callback_url": "",
        "country_code": country_code,
        "channel": "sms",
        "sender_name": "EcoaSupport"
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

paygo_data = calculate_expiry_wallet(modified_data)

post_to_paygo_db(paygo_data)

# for row in modified_data:
#     print(row)

