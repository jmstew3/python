import os
import pyodbc

# Get the environment variables for connection
sql_server = os.getenv('AZURE_SQL_SERVER')
sql_database = os.getenv('AZURE_SQL_DATABASE')
sql_username = os.getenv('AZURE_SQL_USERNAME')
sql_password = os.getenv('AZURE_SQL_PASSWORD')

# Connection string
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

# Sample data to insert (make sure the data aligns with the table structure)
data = [
    {
        'age': 25,
        'allpro': 1,
        'car_av': 10.0,
        'category': 'RB',
        'cfb_player_id': '12345',
        'college': 'College Name',
        'def_ints': 0,
        'def_sacks': 0,
        'def_solo_tackles': 0,
        'dr_av': 5.0,
        'games': 16,
        'gsis_id': '67890',
        'hof': 0,
        'pass_attempts': 350,
        'pass_completions': 200,
        'pass_ints': 10,
        'pass_tds': 25,
        'pass_yards': 3500,
        'pfr_player_id': 'player123',
        'pfr_player_name': 'Player Name',
        'pick': 10,
        'position': 'QB',
        'probowls': 1,
        'rec_tds': 0,
        'rec_yards': 0,
        'receptions': 0,
        'round': 1,
        'rush_atts': 50,
        'rush_tds': 5,
        'rush_yards': 200,
        'season': 2024,
        'seasons_started': 2,
        'side': 'O',
        'team': 'STE',
        '[to]': 2,
        'w_av': 7.0
    }
]

# Insert data
for player in data:
    cursor.execute("""
    INSERT INTO NFLRoster (age, allpro, car_av, category, cfb_player_id, college, def_ints, def_sacks, def_solo_tackles, dr_av, games, gsis_id, hof, pass_attempts, pass_completions, pass_ints, pass_tds, pass_yards, pfr_player_id, pfr_player_name, pick, position, probowls, rec_tds, rec_yards, receptions, round, rush_atts, rush_tds, rush_yards, season, seasons_started, side, team, [to], w_av)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, 
    player['age'], player['allpro'], player['car_av'], player['category'], player['cfb_player_id'], player['college'], player['def_ints'], player['def_sacks'], player['def_solo_tackles'], player['dr_av'], player['games'], player['gsis_id'], player['hof'], player['pass_attempts'], player['pass_completions'], player['pass_ints'], player['pass_tds'], player['pass_yards'], player['pfr_player_id'], player['pfr_player_name'], player['pick'], player['position'], player['probowls'], player['rec_tds'], player['rec_yards'], player['receptions'], player['round'], player['rush_atts'], player['rush_tds'], player['rush_yards'], player['season'], player['seasons_started'], player['side'], player['team'], player['[to]'], player['w_av'])

conn.commit()

# Close the connection
cursor.close()
conn.close()