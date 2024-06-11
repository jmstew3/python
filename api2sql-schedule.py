import os
import pyodbc
import requests
from dotenv import load_dotenv
from datetime import datetime
import json

# Load environment variables from .env file
load_dotenv()

# Azure SQL Database credentials from environment variables
sql_server = os.getenv('AZURE_SQL_SERVER')
sql_database = os.getenv('AZURE_SQL_DATABASE')
sql_username = os.getenv('AZURE_SQL_USERNAME')
sql_password = os.getenv('AZURE_SQL_PASSWORD')

# API endpoint
api_url = 'https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/TEN/schedule?seasontype=2'

# Connection string for SQL Server
conn_str = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={sql_server};'
    f'DATABASE={sql_database};'
    f'UID={sql_username};'
    f'PWD={sql_password}'
)

try:
    # Fetch data from the API
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()

    # Connect to the database
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Connected to SQL Server")

    # Merge data into SQL Database
    merge_query = """
    MERGE NFLSchedule AS target
    USING (SELECT ? AS EventId, ? AS EventDate, ? AS EventName, ? AS ShortName, ? AS SeasonYear, ? AS SeasonDisplayName, 
                  ? AS SeasonType, ? AS WeekNumber, ? AS WeekText, ? AS HomeTeamId, ? AS HomeTeamLocation, ? AS HomeTeamNickname, 
                  ? AS HomeTeamAbbreviation, ? AS HomeTeamDisplayName, ? AS HomeTeamShortDisplayName, ? AS AwayTeamId, 
                  ? AS AwayTeamLocation, ? AS AwayTeamNickname, ? AS AwayTeamAbbreviation, ? AS AwayTeamDisplayName, 
                  ? AS AwayTeamShortDisplayName, ? AS Venue, ? AS City, ? AS State, ? AS ZipCode, ? AS GameTime, ? AS BroadcastChannel, 
                  ? AS HomeTeamLogo, ? AS HomeTeamLogoParams, ? AS AwayTeamLogo, ? AS AwayTeamLogoParams) AS source
    ON (target.EventId = source.EventId)
    WHEN MATCHED THEN 
        UPDATE SET EventDate = source.EventDate, EventName = source.EventName, ShortName = source.ShortName, 
                   SeasonYear = source.SeasonYear, SeasonDisplayName = source.SeasonDisplayName, SeasonType = source.SeasonType, 
                   WeekNumber = source.WeekNumber, WeekText = source.WeekText, HomeTeamId = source.HomeTeamId, 
                   HomeTeamLocation = source.HomeTeamLocation, HomeTeamNickname = source.HomeTeamNickname, 
                   HomeTeamAbbreviation = source.HomeTeamAbbreviation, HomeTeamDisplayName = source.HomeTeamDisplayName, 
                   HomeTeamShortDisplayName = source.HomeTeamShortDisplayName, AwayTeamId = source.AwayTeamId, 
                   AwayTeamLocation = source.AwayTeamLocation, AwayTeamNickname = source.AwayTeamNickname, 
                   AwayTeamAbbreviation = source.AwayTeamAbbreviation, AwayTeamDisplayName = source.AwayTeamDisplayName, 
                   AwayTeamShortDisplayName = source.AwayTeamShortDisplayName, Venue = source.Venue, City = source.City, 
                   State = source.State, ZipCode = source.ZipCode, GameTime = source.GameTime, BroadcastChannel = source.BroadcastChannel, 
                   HomeTeamLogo = source.HomeTeamLogo, HomeTeamLogoParams = source.HomeTeamLogoParams, AwayTeamLogo = source.AwayTeamLogo, 
                   AwayTeamLogoParams = source.AwayTeamLogoParams
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (EventId, EventDate, EventName, ShortName, SeasonYear, SeasonDisplayName, SeasonType, WeekNumber, WeekText, HomeTeamId, 
                HomeTeamLocation, HomeTeamNickname, HomeTeamAbbreviation, HomeTeamDisplayName, HomeTeamShortDisplayName, AwayTeamId, 
                AwayTeamLocation, AwayTeamNickname, AwayTeamAbbreviation, AwayTeamDisplayName, AwayTeamShortDisplayName, Venue, City, 
                State, ZipCode, GameTime, BroadcastChannel, HomeTeamLogo, HomeTeamLogoParams, AwayTeamLogo, AwayTeamLogoParams)
        VALUES (source.EventId, source.EventDate, source.EventName, source.ShortName, source.SeasonYear, source.SeasonDisplayName, 
                source.SeasonType, source.WeekNumber, source.WeekText, source.HomeTeamId, source.HomeTeamLocation, source.HomeTeamNickname, 
                source.HomeTeamAbbreviation, source.HomeTeamDisplayName, source.HomeTeamShortDisplayName, source.AwayTeamId, 
                source.AwayTeamLocation, source.AwayTeamNickname, source.AwayTeamAbbreviation, source.AwayTeamDisplayName, 
                source.AwayTeamShortDisplayName, source.Venue, source.City, source.State, source.ZipCode, source.GameTime, source.BroadcastChannel, 
                source.HomeTeamLogo, source.HomeTeamLogoParams, source.AwayTeamLogo, source.AwayTeamLogoParams);
    """

    # Data insertion logic
    for event in data['events']:
        event_id = event.get('id')
        event_date_str = event.get('date')
        event_date = datetime.strptime(event_date_str, '%Y-%m-%dT%H:%MZ')
        event_name = event.get('name')
        short_name = event.get('shortName')
        season_year = event['season'].get('year')
        season_display_name = event['season'].get('displayName')
        season_type = event['seasonType'].get('name')
        week_number = event['week'].get('number')
        week_text = event['week'].get('text')
        
        home_team = next(comp for comp in event['competitions'][0]['competitors'] if comp['homeAway'] == 'home')['team']
        away_team = next(comp for comp in event['competitions'][0]['competitors'] if comp['homeAway'] == 'away')['team']
        
        home_team_id = home_team.get('id')
        home_team_location = home_team.get('location')
        home_team_nickname = home_team.get('nickname')
        home_team_abbreviation = home_team.get('abbreviation')
        home_team_display_name = home_team.get('displayName')
        home_team_short_display_name = home_team.get('shortDisplayName')
        
        home_team_logo = home_team.get('logos', [])[0].get('href') if home_team.get('logos') else None
        home_team_logo_params = json.dumps(home_team.get('logos', [])[0]) if home_team.get('logos') else None
        
        away_team_id = away_team.get('id')
        away_team_location = away_team.get('location')
        away_team_nickname = away_team.get('nickname')
        away_team_abbreviation = away_team.get('abbreviation')
        away_team_display_name = away_team.get('displayName')
        away_team_short_display_name = away_team.get('shortDisplayName')
        
        away_team_logo = away_team.get('logos', [])[0].get('href') if away_team.get('logos') else None
        away_team_logo_params = json.dumps(away_team.get('logos', [])[0]) if away_team.get('logos') else None
        
        venue = event['competitions'][0]['venue'].get('fullName')
        city = event['competitions'][0]['venue']['address'].get('city')
        state = event['competitions'][0]['venue']['address'].get('state')
        zip_code = event['competitions'][0]['venue']['address'].get('zipCode')
        
        game_time = event_date_str.split('T')[1].split('Z')[0]
        broadcast_channel = event['competitions'][0]['broadcasts'][0]['media'].get('shortName', 'N/A') if event['competitions'][0]['broadcasts'] else 'N/A'
        
        cursor.execute(merge_query, (
            event_id, event_date, event_name, short_name, season_year, season_display_name, 
            season_type, week_number, week_text, home_team_id, home_team_location, home_team_nickname, 
            home_team_abbreviation, home_team_display_name, home_team_short_display_name, away_team_id, 
            away_team_location, away_team_nickname, away_team_abbreviation, away_team_display_name, 
            away_team_short_display_name, venue, city, state, zip_code, game_time, broadcast_channel, 
            home_team_logo, home_team_logo_params, away_team_logo, away_team_logo_params
        ))
        conn.commit()  # Commit each insert

    print("Data inserted successfully")

except requests.exceptions.RequestException as e:
    print(f"Error fetching data from API: {e}")
except pyodbc.Error as e:
    print(f"Error connecting to SQL Server: {e}")
finally:
    # Close the connection
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()