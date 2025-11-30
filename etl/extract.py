import os 
import json
import requests # type: ignore
from datetime import datetime


def build_weather_url(latitude: float, longitude: float):
    """_summary_

    Args:
        latitude (float): _description_
        longitude (float): _description_

    Returns:
        _type_: _description_
    """
    base_url = "https://api.open-meteo.com/v1/forecast"
    params = (
        f"?latitude={latitude}"
        f"&longitude={longitude}"
        f"&hourly=temperature_2m,relativehumidity_2m,precipitation"
    )

    return base_url + params 

def extract_weather_data(latitude: float, longitude: float, raw_path: str) -> str:
    """_summary_

    Args:
        latitude (float): _description_
        longitude (float): _description_
        raw_path (str): _description_

    Raises:
        Exception: _description_

    Returns:
        str: _description_
    """
    url = build_weather_url(latitude,longitude)
    print(f"Requesting Weather Data from: {url}")

    response = requests.get(url, timeout=10)

    # Basic validation of response
    if response.status_code != 200: 
        raise Exception(f"API request failed: {response.status_code} - {response.text}")
    
    data = response.json()

    # Ensure raw directory exists
    os.makedirs(raw_path, exist_ok=True)

    # Build filename like: weather_raw_2023-10-05T14-30-00.json
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    filename = f"weather_raw_{timestamp}.json"
    file_path = os.path.join(raw_path, filename)

    # Save JSON data to file 
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"Raw Data save to: {file_path}")
    return file_path