from flask import Flask, jsonify, request
import pandas as pd
import json
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from flask_cors import CORS
import os

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Initialize BlobServiceClient
connection_string = os.getenv('AZURE_CONNECTION_STRING')
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

@app.route('/fetch-draft-data', methods=['GET'])
def fetch_draft_data():
    url = 'https://github.com/nflverse/nflverse-data/releases/download/draft_picks/draft_picks.csv'
    draft_data = pd.read_csv(url)
    draft_data_json = draft_data.to_json(orient='records')
    return jsonify(json.loads(draft_data_json))

@app.route('/fetch-roster-data', methods=['GET'])
def fetch_roster_data():
    url = 'https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_2024.csv'
    roster_data = pd.read_csv(url)
    roster_data_json = roster_data.to_json(orient='records')
    return jsonify(json.loads(roster_data_json))

@app.route('/upload-draft-data', methods=['POST'])
def upload_draft_data():
    container_name = 'nfl-draft-data'
    blob_name = 'draft_picks_1980-2024.json'
    
    container_client = blob_service_client.get_container_client(container_name)
    
    try:
        # Create the container if it doesn't exist
        container_client.create_container()
    except Exception as e:
        print(f"Container already exists or cannot be created: {e}")
    
    # Get JSON data from request
    draft_data_json = request.get_json()
    
    # Upload the JSON data to Blob Storage
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json.dumps(draft_data_json), overwrite=True)
    
    return jsonify({'message': 'Draft data uploaded successfully'})

@app.route('/upload-roster-data', methods=['POST'])
def upload_roster_data():
    container_name = 'nfl-roster-data'
    blob_name = 'roster_2024.json'
    
    container_client = blob_service_client.get_container_client(container_name)
    
    try:
        # Create the container if it doesn't exist
        container_client.create_container()
    except Exception as e:
        print(f"Container already exists or cannot be created: {e}")
    
    # Get JSON data from request
    roster_data_json = request.get_json()
    
    # Upload the JSON data to Blob Storage
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json.dumps(roster_data_json), overwrite=True)
    
    return jsonify({'message': 'Roster data uploaded successfully'})

@app.route('/get-draft-data', methods=['GET'])
def get_draft_data():
    container_name = 'nfl-draft-data'
    blob_name = 'draft_picks_1980-2024.json'
    
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)
    
    # Download the blob's contents as a string
    blob_data = blob_client.download_blob().readall()
    draft_data = json.loads(blob_data)
    
    return jsonify(draft_data)

@app.route('/get-roster-data', methods=['GET'])
def get_roster_data():
    container_name = 'nfl-roster-data'
    blob_name = 'roster_2024.json'
    
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)
    
    # Download the blob's contents as a string
    blob_data = blob_client.download_blob().readall()
    roster_data = json.loads(blob_data)
    
    return jsonify(roster_data)

if __name__ == '__main__':
    app.run(debug=True, port=5001)