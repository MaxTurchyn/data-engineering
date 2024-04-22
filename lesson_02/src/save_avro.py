from fastapi import HTTPException
import os
import json
from fastavro import writer, parse_schema


def save_avro(raw_dir: str, stg_dir: str):
    try:
        # Ensure the directories exist
        if not os.path.exists(raw_dir):
            raise HTTPException(status_code=400, detail=f"Raw directory '{raw_dir}' does not exist")
        if not os.path.exists(stg_dir):
            os.makedirs(raw_dir, exist_ok=True)

        # List all JSON files in the raw directory
        json_files = [f for f in os.listdir(raw_dir) if f.endswith(".json")]

        # Create the stg directory
        os.makedirs(stg_dir, exist_ok=True)

        # Clear the contents of the directory
        for file_name in os.listdir(stg_dir):
            file_path = os.path.join(stg_dir, file_name)
            try:
                os.unlink(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")

        schema = {
            'name': 'Client cards',
            'type': 'record',
            'fields': [
                {'name': 'client', 'type': 'string'},
                {'name': 'purchase_date', 'type': 'string'},
                {'name': 'product', 'type': 'string'},
                {'name': 'price', 'type': 'int'},
            ],
        }

        parsed_schema = parse_schema(schema)

        # Process each JSON file and write to Avro
        for json_file in json_files:
            json_file_path = os.path.join(raw_dir, json_file)
            avro_file_name = json_file.replace(".json", ".avro")
            avro_file_path = os.path.join(stg_dir, avro_file_name)

            # Read JSON data
            with open(json_file_path, "r") as file:
                json_data = json.load(file)

            # Write Avro file
            with open(avro_file_path, "wb") as avro_file:
                writer(avro_file, parsed_schema, json_data)
                print(f"Data saved to {avro_file}")

        return {"message": "Avro files written successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# To make it visible in pull request