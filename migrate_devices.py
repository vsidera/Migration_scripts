import psycopg2
from datetime import datetime, timedelta

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
                accounts.id,
                accounts.external_id,
                accounts.serial_number
            FROM accounts
            WHERE accounts.serial_number IS NOT NULL AND accounts.serial_number <> ''
            ORDER BY accounts.created_at DESC
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
                    total_repayment_derived
                FROM m_loan
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

    for row in modified_data:

        account_id = row[0]
        external_id = row[1]
        serial_number = row[2]
        disbursed_on_date = row[3]
        repay_every = row[4]
        expected_matured_on_date = row[5]
        deposit = row[6]
        principal = row[7]
        total_paid = row[8]

        today = datetime.now().date()
        date_disbursed = datetime.strptime(disbursed_on_date, '%Y-%m-%d').date()
        days_since_deposit = today - date_disbursed

        maturity_date = datetime.strptime(expected_matured_on_date, '%Y-%m-%d').date()
        total_loan_period = maturity_date - date_disbursed

        amount_paid = total_paid - deposit
        amt_to_be_on_track = (days_since_deposit/total_loan_period)*principal

        deficit = amount_paid-amt_to_be_on_track

        if deficit <= 0:
            expiry_date = '2022-07-10'
        else :
            expiry_date =  floor (amount_paid - amt_to_be_on_track)/daily rate + July 10th


        print(row[1])

data = fetch_accounts_from_crm()

modified_data = fetch_figures_from_fineract(data)

calculate_expiry_wallet(modified_data)

# for row in modified_data:
#     print(row)

