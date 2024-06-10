import os
import pyodbc
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

# TRANSFER JSON DATA FROM NFL-DRAFT-DATA CONTAINER TO AZURE SQL
# Azure Storage credentials
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
container_name = 'nfl-draft-data'
blob_name = 'draft_picks_1980-2024.json'

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
    INSERT INTO NFLDraft (season, round, pick, team, gsis_id, pfr_player_id, cfb_player_id, pfr_player_name, hof, position, category, side, college, age, [to], allpro, probowls, seasons_started, w_av, car_av, dr_av, games, pass_completions, pass_attempts, pass_yards, pass_tds, pass_ints, rush_atts, rush_yards, rush_tds, receptions, rec_yards, rec_tds, def_solo_tackles, def_ints, def_sacks)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for player in data:
        try:
            if player['pfr_player_id']:
                # Check if the record already exists
                cursor.execute("SELECT COUNT(*) FROM NFLDraft WHERE pfr_player_id = ?", player['pfr_player_id'])
                if cursor.fetchone()[0] == 0:  # If no existing record is found
                    cursor.execute(insert_query, (
                        player.get('season', None), player.get('round', None), player.get('pick', None), player.get('team', None),
                        player.get('gsis_id', None), player.get('pfr_player_id', None), player.get('cfb_player_id', None), player.get('pfr_player_name', None),
                        player.get('hof', None), player.get('position', None), player.get('category', None), player.get('side', None),
                        player.get('college', None), player.get('age', None), player.get('to', None), player.get('allpro', None),
                        player.get('probowls', None), player.get('seasons_started', None), player.get('w_av', None), player.get('car_av', None),
                        player.get('dr_av', None), player.get('games', None), player.get('pass_completions', None), player.get('pass_attempts', None),
                        player.get('pass_yards', None), player.get('pass_tds', None), player.get('pass_ints', None), player.get('rush_atts', None),
                        player.get('rush_yards', None), player.get('rush_tds', None), player.get('receptions', None), player.get('rec_yards', None),
                        player.get('rec_tds', None), player.get('def_solo_tackles', None), player.get('def_ints', None), player.get('def_sacks', None)
                    ))
                    conn.commit()  # Commit each insert
        except pyodbc.Error as e:
            print(f"Error processing player {player.get('pfr_player_id', 'Unknown')}: {e}")
        except Exception as e:
            print(f"Unexpected error processing player {player.get('pfr_player_id', 'Unknown')}: {e}")

    print("Data inserted successfully")

except pyodbc.Error as e:
    print(f"Error connecting to SQL Server: {e}")

finally:
    # Close the connection
    cursor.close()
    conn.close()