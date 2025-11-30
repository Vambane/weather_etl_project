import json 
import pandas as pd  # type: ignore
from datetime import datetime

def load_raw_json(file_path: str) -> dict: 
    with open(file_path, "r") as f:
        return json.load(f)
    
def transform_weather_data(raw_json: dict, latitude: float, longitude: float) -> pd.DataFrame:

    hourly = raw_json["hourly"]

    df = pd.DataFrame({
        "timestamp": hourly["time"],
        "temperature_2m": hourly["temperature_2m"],
        "relativehumidity_2m": hourly["relativehumidity_2m"],
        "precipitation": hourly["precipitation"]
    })

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # add metadata
    df["latitude"] = latitude
    df["longitude"] = longitude
    df["load_date"] = datetime.utcnow()

    return df