import requests
import psycopg2
from datetime import datetime

# Define the Agents struct
class Agents:
    def __init__(self, external_id, name):
        self.ExternalID = external_id
        self.Name = name

# API endpoint URL
api_url = "https://faad3nwqfw.us-east-1.awsapprunner.com/api/agents"

# Database connection string
db_connection_string = "postgres://salesapp:gx40E2t5WcBq@burn-ecore-production.cluster-cbxsbosv8vtr.eu-west-1.rds.amazonaws.com:5432/customerdb?sslmode=require"

def fetch_agents_data():
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data from the API.")
        return []

def map_data_to_agents(data):
   
    agents = []
    for item in data:
        external_id = item["id"]
        first_name = item["user_first_name"]
        last_name = item["user_last_name"]
        name = f"{first_name} {last_name}"
        agent = Agents(external_id=external_id, name=name)
        agents.append(agent)
    return agents

def update_agents_data(agents):
    try:
        conn = psycopg2.connect(db_connection_string)
        cursor = conn.cursor()

        for agent in agents:
            query = """
                UPDATE sales_agents
                SET name = %s
                WHERE external_id = %s
            """
            values = (agent.Name, agent.ExternalID)
            cursor.execute(query, values)

        conn.commit()
        print("Data updated successfully.")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        if conn:
            cursor.close()
            conn.close()

# Fetch data from the API
data = fetch_agents_data()

# Map data to Agents objects
agents = map_data_to_agents(data)

# Update agents data in the database
update_agents_data(agents)