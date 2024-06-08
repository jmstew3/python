from flask import Flask, jsonify, request
import pandas as pd
import json
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from flask_cors import CORS
import os
import azure.core.exceptions

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

@app.route('/fetch-and-upload-draft-data', methods=['POST'])
def fetch_and_upload_draft_data():
    # Fetch draft data from the URL
    url = 'https://github.com/nflverse/nflverse-data/releases/download/draft_picks/draft_picks.csv'
    draft_data = pd.read_csv(url)
    draft_data_json = draft_data.to_json(orient='records')
    draft_data_dict = json.loads(draft_data_json)

    # Upload the fetched data to Azure Blob Storage
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    container_name = 'nfl-draft-data'
    blob_name = 'draft_picks_1980-2024.json'

    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    try:
        # Create the container if it doesn't exist
        container_client.create_container()
    except Exception as e:
        print(f"Container already exists or cannot be created: {e}")

    # Upload the JSON data to Blob Storage
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json.dumps(draft_data_dict), overwrite=True)

    return jsonify({'message': 'Data fetched and uploaded successfully'})

@app.route('/get-draft-data', methods=['GET'])
def get_draft_data():
    connection_string = os.getenv('AZURE_CONNECTION_STRING')
    container_name = 'nfl-draft-data'
    blob_name = 'draft_picks_1980-2024.json'

    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)

    try:
        # Download the blob's contents as a string
        blob_data = blob_client.download_blob().readall()

        # Check if the blob data is empty
        if not blob_data:
            print(f"Blob {blob_name} is empty")
            return jsonify({"error": "The blob is empty."}), 404

        # Attempt to parse the blob data as JSON
        try:
            draft_data = json.loads(blob_data)
        except json.JSONDecodeError as json_error:
            print(f"Error decoding JSON: {json_error}")
            return jsonify({"error": "Failed to parse JSON data from the blob."}), 500

        return jsonify(draft_data)
    except azure.core.exceptions.ResourceNotFoundError:
        print(f"Blob {blob_name} not found in container {container_name}")
        return jsonify({"error": "The specified blob does not exist."}), 404
    except Exception as e:
        print(f"An error occurred: {e}")  # Print the actual error message
        return jsonify({"error": f"An error occurred while fetching the data: {e}"}), 500

@app.route('/fetch-and-upload-roster-data', methods=['POST'])
def fetch_and_upload_roster_data():
    url = 'https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_2024.csv'
    try:
        # Fetch the CSV data from the URL
        roster_data = pd.read_csv(url)
        # Convert the data to JSON format
        roster_data_json = roster_data.to_json(orient='records')
        
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        container_name = 'nfl-roster-data'
        blob_name = 'roster_2024.json'
        
        # Initialize BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        
        try:
            # Create the container if it doesn't exist
            container_client.create_container()
        except Exception as e:
            print(f"Container already exists or cannot be created: {e}")
        
        # Upload the JSON data to Blob Storage
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(roster_data_json, overwrite=True)
        
        return jsonify({'message': 'Data fetched and uploaded successfully'})
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({'error': f'An error occurred: {e}'}), 500
    
@app.route('/get-roster-data', methods=['GET'])
def get_roster_data():
    connection_string = os.getenv('AZURE_CONNECTION_STRING')
    container_name = 'nfl-roster-data'
    blob_name = 'roster_2024.json'

    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)

    try:
        # Download the blob's contents as a string
        blob_data = blob_client.download_blob().readall()

        # Check if the blob data is empty
        if not blob_data:
            print(f"Blob {blob_name} is empty")
            return jsonify({"error": "The blob is empty."}), 404

        # Attempt to parse the blob data as JSON
        try:
            roster_data = json.loads(blob_data)
        except json.JSONDecodeError as json_error:
            print(f"Error decoding JSON: {json_error}")
            return jsonify({"error": "Failed to parse JSON data from the blob."}), 500

        return jsonify(roster_data)
    except azure.core.exceptions.ResourceNotFoundError:
        print(f"Blob {blob_name} not found in container {container_name}")
        return jsonify({"error": "The specified blob does not exist."}), 404
    except Exception as e:
        print(f"An error occurred: {e}")  # Print the actual error message
        return jsonify({"error": f"An error occurred while fetching the data: {e}"}), 500

@app.route('/get-schedule-data', methods=['GET'])
def get_schedule_data():
    connection_string = os.getenv('AZURE_CONNECTION_STRING')
    container_name = 'nflschedule1e835d00-19e7-11ef-bf0e-0ddb63396b97'
    blob_name = 'TEN_schedule_2024.json'

    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)

    try:
        # Download the blob's contents as a string
        blob_data = blob_client.download_blob().readall()

        # Check if the blob data is empty
        if not blob_data:
            print(f"Blob {blob_name} is empty")
            return jsonify({"error": "The blob is empty."}), 404

        # Attempt to parse the blob data as JSON
        try:
            schedule_data = json.loads(blob_data)
        except json.JSONDecodeError as json_error:
            print(f"Error decoding JSON: {json_error}")
            return jsonify({"error": "Failed to parse JSON data from the blob."}), 500

        return jsonify(schedule_data)
    except azure.core.exceptions.ResourceNotFoundError:
        print(f"Blob {blob_name} not found in container {container_name}")
        return jsonify({"error": "The specified blob does not exist."}), 404
    except Exception as e:
        print(f"An error occurred: {e}")  # Print the actual error message
        return jsonify({"error": f"An error occurred while fetching the data: {e}"}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)