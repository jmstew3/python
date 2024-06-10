import os
import pyodbc
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

# TRANSFER JSON DATA FROM NFL-ROSTER-DATA CONTAINER TO AZURE SQL
# Azure Storage credentials
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
container_name = 'nfl-roster-data'
blob_name = 'roster_2024.json'

# Azure SQL Database credentials from environment variables
sql_server = os.getenv('AZURE_SQL_SERVER')
sql_database = os.getenv('AZURE_SQL_DATABASE')
sql_username = os.getenv('AZURE_SQL_USERNAME')
sql_password = os.getenv('AZURE_SQL_PASSWORD')

# Connect to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# Read data from the blob
blob_data = blob_client.download_blob().readall()
data = json.loads(blob_data)

# Connection string for SQL Server
conn_str = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={sql_server};'
    f'DATABASE={sql_database};'
    f'UID={sql_username};'
    f'PWD={sql_password}'
)

try:
    # Connect to the database
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Connected to SQL Server")

    # Insert data into SQL Database
    insert_query = """
    INSERT INTO NFLRoster (birth_date, college, depth_chart_position, draft_club, draft_number, entry_year, esb_id, espn_id, fantasy_data_id, first_name, football_name, full_name, game_type, gsis_id, gsis_it_id, headshot_url, height, jersey_number, last_name, ngs_position, pff_id, pfr_id, position, rookie_year, rotowire_id, season, sleeper_id, smart_id, sportradar_id, status, status_description_abbr, team, week, weight, yahoo_id, years_exp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for player in data:
        try:
            if player['gsis_id']:  # Ensure 'gsis_id' is not null
                cursor.execute(insert_query, (
                    player.get('birth_date', None), player.get('college', None), player.get('depth_chart_position', None), player.get('draft_club', None),
                    player.get('draft_number', None), player.get('entry_year', None), player.get('esb_id', None), player.get('espn_id', None),
                    player.get('fantasy_data_id', None), player.get('first_name', None), player.get('football_name', None), player.get('full_name', None),
                    player.get('game_type', None), player.get('gsis_id', None), player.get('gsis_it_id', None), player.get('headshot_url', None),
                    player.get('height', None), player.get('jersey_number', None), player.get('last_name', None), player.get('ngs_position', None),
                    player.get('pff_id', None), player.get('pfr_id', None), player.get('position', None), player.get('rookie_year', None),
                    player.get('rotowire_id', None), player.get('season', None), player.get('sleeper_id', None), player.get('smart_id', None),
                    player.get('sportradar_id', None), player.get('status', None), player.get('status_description_abbr', None), player.get('team', None),
                    player.get('week', None), player.get('weight', None), player.get('yahoo_id', None), player.get('years_exp', None)
                ))
                conn.commit()  # Commit each insert
        except pyodbc.Error as e:
            print(f"Error processing player {player.get('gsis_id', 'Unknown')}: {e}")
        except Exception as e:
            print(f"Unexpected error processing player {player.get('gsis_id', 'Unknown')}: {e}")

    print("Data inserted successfully")

except pyodbc.Error as e:
    print(f"Error connecting to SQL Server: {e}")

finally:
    # Close the connection
    cursor.close()
    conn.close()