# app.py
from flask import Flask, jsonify, request
import pandas as pd
import json
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
app = Flask(__name__)

@app.route('/fetch-draft-data', methods=['GET'])
def fetch_draft_data():
    url = 'https://github.com/nflverse/nflverse-data/releases/download/draft_picks/draft_picks.csv'
    draft_data = pd.read_csv(url)
    draft_data_json = draft_data.to_json(orient='records')
    return jsonify(json.loads(draft_data_json))

@app.route('/upload-draft-data', methods=['POST'])
def upload_draft_data():    
    connection_string = os.getenv('AZURE_CONNECTION_STRING')
    container_name = 'nfl-draft-data'
    blob_name = 'draft_picks_1980-2024.json'
    
    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    # Create the container if it doesn't exist
    container_client.create_container()
    
    # Get JSON data from request
    draft_data_json = request.get_json()
    
    # Upload the JSON data to Blob Storage
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json.dumps(draft_data_json), overwrite=True)
    
    return jsonify({'message': 'Data uploaded successfully'})

if __name__ == '__main__':
    app.run(debug=True)