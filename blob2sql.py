import os
import pyodbc
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

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

# Connect to the database
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Insert data into SQL Database
for player in data:
    if player['pfr_player_id']:  # Ensure 'pfr_player_id' is not null
        # Check if the record already exists
        cursor.execute("SELECT COUNT(*) FROM NFLDraft WHERE pfr_player_id = ?", player['pfr_player_id'])
        if cursor.fetchone()[0] == 0:  # If no existing record is found
            cursor.execute("""
            INSERT INTO NFLDraft (age, allpro, car_av, category, cfb_player_id, college, def_ints, def_sacks, def_solo_tackles, dr_av, games, gsis_id, hof, pass_attempts, pass_completions, pass_ints, pass_tds, pass_yards, pfr_player_id, pfr_player_name, pick, position, probowls, rec_tds, rec_yards, receptions, round, rush_atts, rush_tds, rush_yards, season, seasons_started, side, team, [to], w_av)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, 
            player.get('age', None), player.get('allpro', None), player.get('car_av', None), player.get('category', None), player.get('cfb_player_id', None), player.get('college', None), player.get('def_ints', None), player.get('def_sacks', None), player.get('def_solo_tackles', None), player.get('dr_av', None), player.get('games', None), player.get('gsis_id', None), player.get('hof', None), player.get('pass_attempts', None), player.get('pass_completions', None), player.get('pass_ints', None), player.get('pass_tds', None), player.get('pass_yards', None), player.get('pfr_player_id', None), player.get('pfr_player_name', None), player.get('pick', None), player.get('position', None), player.get('probowls', None), player.get('rec_tds', None), player.get('rec_yards', None), player.get('receptions', None), player.get('round', None), player.get('rush_atts', None), player.get('rush_tds', None), player.get('rush_yards', None), player.get('season', None), player.get('seasons_started', None), player.get('side', None), player.get('team', None), player.get('to', None), player.get('w_av', None))

conn.commit()

# Close the connection
cursor.close()
conn.close()