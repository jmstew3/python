import os
import pyodbc
import json
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load environment variables
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
sql_server = os.getenv('AZURE_SQL_SERVER')
sql_database = os.getenv('AZURE_SQL_DATABASE')
sql_username = os.getenv('AZURE_SQL_USERNAME')
sql_password = os.getenv('AZURE_SQL_PASSWORD')

# Blob Storage connection
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_name = 'nfl-roster-data'
blob_name = 'roster_2024.json'
blob_client = blob_service_client.get_blob_client(container_name, blob_name)

# Fetch data from blob
blob_data = blob_client.download_blob().readall()
roster_data = json.loads(blob_data)

# SQL Server connection string
conn_str = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={sql_server};'
    f'DATABASE={sql_database};'
    f'UID={sql_username};'
    f'PWD={sql_password};'
    'Connection Timeout=30;'
)

# SQL table creation query
create_table_query = '''
CREATE TABLE NFLDraft (
    age FLOAT,
    allpro INT,
    car_av FLOAT,
    category VARCHAR(50),
    cfb_player_id VARCHAR(50),
    college VARCHAR(50),
    def_ints FLOAT,
    def_sacks FLOAT,
    def_solo_tackles FLOAT,
    dr_av FLOAT,
    games FLOAT,
    gsis_id VARCHAR(50),
    hof BIT,
    pass_attempts FLOAT,
    pass_completions FLOAT,
    pass_ints FLOAT,
    pass_tds FLOAT,
    pass_yards FLOAT,
    pfr_player_id VARCHAR(50) PRIMARY KEY,
    pfr_player_name VARCHAR(50),
    pick INT,
    position VARCHAR(50),
    probowls INT,
    rec_tds FLOAT,
    rec_yards FLOAT,
    receptions FLOAT,
    round INT,
    rush_atts FLOAT,
    rush_tds FLOAT,
    rush_yards FLOAT,
    season INT,
    seasons_started INT,
    side VARCHAR(1),
    team VARCHAR(3),
    [to] FLOAT,
    w_av FLOAT
);
'''

# Connect to SQL Server and create table if it doesn't exist
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Connected to SQL Server")
    
    # Check if table exists, and create it if it doesn't
    try:
        cursor.execute("SELECT 1 FROM NFLRoster")
    except pyodbc.Error as e:
        if "Invalid object name 'NFLRoster'" in str(e):
            cursor.execute(create_table_query)
            conn.commit()
            print("NFLRoster table created")
        else:
            raise e
    
    # Insert data into SQL Server
    sql_insert = '''
        IF NOT EXISTS (SELECT 1 FROM NFLRoster WHERE pfr_player_id = ?)
        BEGIN
            INSERT INTO NFLRoster (
                age, allpro, car_av, category, cfb_player_id, college, def_ints, def_sacks, def_solo_tackles, 
                dr_av, games, gsis_id, hof, pass_attempts, pass_completions, pass_ints, pass_tds, pass_yards, 
                pfr_player_id, pfr_player_name, pick, position, probowls, rec_tds, rec_yards, receptions, 
                round, rush_atts, rush_tds, rush_yards, season, seasons_started, side, team, [to], w_av
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        END
    '''
    params = [
        'age', 'allpro', 'car_av', 'category', 'cfb_player_id', 'college', 'def_ints', 'def_sacks', 'def_solo_tackles', 
        'dr_av', 'games', 'gsis_id', 'hof', 'pass_attempts', 'pass_completions', 'pass_ints', 'pass_tds', 'pass_yards', 
        'pfr_player_id', 'pfr_player_name', 'pick', 'position', 'probowls', 'rec_tds', 'rec_yards', 'receptions', 
        'round', 'rush_atts', 'rush_tds', 'rush_yards', 'season', 'seasons_started', 'side', 'team', 'to', 'w_av'
    ]

    for record in roster_data:
        try:
            cursor.execute(sql_insert, [record.get(param, None) for param in params])
        except Exception as e:
            print(f"Error inserting record: {e}")
    
    conn.commit()
    conn.close()
    print("Data inserted successfully")
except pyodbc.Error as e:
    print("Error connecting to SQL Server:", e)