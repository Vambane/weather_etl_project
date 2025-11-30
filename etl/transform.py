import json 
import pandas as pd  # type: ignore
from datetime import datetime

def load_raw_json(file_path: str) -> dict: 
    with open(file_path, "r") as f:
        return json.load(f)
    
def transform_weather_data(raw_json: dict, latitude: float, longitude: float) -> pd.DataFrame:

    if "hourly" not in raw_json: 
        raise ValueError("Missing 'hourly' section in the raw JSON")
    
    hourly = raw_json["hourly"]

    required_fields = [
        "time",
        "temperature_2m",
        "relativehumidity_2m",
        "precipitation"
    ]

    # ----------------------------
    # 2. Schema validation
    # ----------------------------
    for field in required_fields:
        if field not in hourly:
            raise ValueError(f"Missing required field: {field}")
    
    # ----------------------------
    # 3. Build DataFrame
    # ----------------------------
    df = pd.DataFrame({
        "timestamp": hourly["time"],
        "temperature_2m": hourly["temperature_2m"],
        "relativehumidity_2m": hourly["relativehumidity_2m"],
        "precipitation": hourly["precipitation"]
    })

    # convert timestamp to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # ----------------------------
    # 4. Data Quality Checks
    # ----------------------------
    # Check for empty dataset
    if df.empty:
        raise ValueError("Transformed DataFrame is empty.")

    # Check for null timestamps
    if df["timestamp"].isna().any():
        raise ValueError("Null timestamps found — invalid source data.")

    # Check for duplicate timestamps
    if df["timestamp"].duplicated().any():
        raise ValueError("Duplicate timestamps detected — failing DQC.")

    # Check numeric ranges (basic sanity checks)
    if (df["temperature_2m"] < -90).any() or (df["temperature_2m"] > 60).any():
        raise ValueError("Temperature values outside realistic range.")

    if (df["relativehumidity_2m"] < 0).any() or (df["relativehumidity_2m"] > 100).any():
        raise ValueError("Humidity values outside 0–100% range.")
    
    if (df["precipitation"] < 0).any():
        raise ValueError("Negative precipitation values found.")
    # add metadata
    df["latitude"] = latitude
    df["longitude"] = longitude
    df["load_date"] = datetime.utcnow().date()

    return df