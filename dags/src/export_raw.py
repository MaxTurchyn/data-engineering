import json
import os
import requests

def export_raw_job(BASE_DIR, API_URL, AUTH_TOKEN, **context):

    logical_date = context['ds']
    raw_dir = os.path.join(BASE_DIR, "raw", "sales", logical_date)

    # Create the directory if it doesn't exist
    os.makedirs(raw_dir, exist_ok=True)

    # Clear the contents of the directory
    for file_name in os.listdir(raw_dir):
        file_path = os.path.join(raw_dir, file_name)
        try:
            os.unlink(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")

    url = API_URL
    headers = {"Authorization": f"{AUTH_TOKEN}"}

    page = 1
    while True:
        params = {"date": f"{logical_date}", "page": page}
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            raise Exception("Failed to fetch data from API")

        page_data = response.json()
        save_path = os.path.join(raw_dir, f"{params['date']}_{params['page']}.json")

        with open(save_path, "w") as file:
            json.dump(page_data, file)
        print(f"Data saved to {save_path}")

        if len(page_data) < 100:
            break

        page += 1