import os
import json
import pandas as pd
from datetime import datetime, timedelta


def load_raw_json(file_path: str) -> dict:
    with open(file_path, "r") as f:
        return json.load(f)


def transform_weather_data(raw_json: dict, latitude: float, longitude: float, city: str) -> pd.DataFrame:

    # ----------------------------
    # 1. Basic structure validation
    # ----------------------------
    if "hourly" not in raw_json:
        raise ValueError("Missing 'hourly' section in the raw JSON")

    hourly = raw_json["hourly"]

    required_fields = [
        "time",
        "temperature_2m",
        "relativehumidity_2m",
        "precipitation"
    ]

    for field in required_fields:
        if field not in hourly:
            raise ValueError(f"Missing required field: {field}")

    # ----------------------------
    # 2. Build DataFrame
    # ----------------------------
    df = pd.DataFrame({
        "timestamp": hourly["time"],
        "temperature_2m": hourly["temperature_2m"],
        "relativehumidity_2m": hourly["relativehumidity_2m"],
        "precipitation": hourly["precipitation"]
    })

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # ----------------------------
    # 3. Data Quality Checks
    # ----------------------------
    if df.empty:
        raise ValueError("Transformed DataFrame is empty.")

    if df["timestamp"].isna().any():
        raise ValueError("Null timestamps found — invalid source data.")

    if df["timestamp"].duplicated().any():
        raise ValueError("Duplicate timestamps detected — failing DQC.")

    if (df["temperature_2m"] < -90).any() or (df["temperature_2m"] > 60).any():
        raise ValueError("Temperature values outside realistic range.")

    if (df["relativehumidity_2m"] < 0).any() or (df["relativehumidity_2m"] > 100).any():
        raise ValueError("Humidity values outside 0–100% range.")

    if (df["precipitation"] < 0).any():
        raise ValueError("Negative precipitation values found.")

    # ----------------------------
    # 4. Freshness Checks
    # ----------------------------

    # 4A: Expected number of hours (7 days = 168)
    expected_hours = 24 * 7
    if len(df) != expected_hours:
        raise ValueError(f"Expected {expected_hours} records, found {len(df)}")

    # 4B: Timestamp gaps
    expected_range = pd.date_range(start=df["timestamp"].min(),
                                   end=df["timestamp"].max(),
                                   freq="H")

    if len(expected_range) != len(df):
        raise ValueError("Timestamp gaps detected. Missing hourly data.")

    # 4C: Staleness check (latest timestamp must be recent)
    latest_ts = df["timestamp"].max()
    now_utc = datetime.utcnow()

    if now_utc - latest_ts > timedelta(hours=6):
        raise ValueError(
            f"Data is stale. Latest timestamp is {latest_ts}, "
            f"{(now_utc - latest_ts).total_seconds()/3600:.1f} hours old."
        )

    # ----------------------------
    # 5. Add metadata
    # ----------------------------
    df["city"] = city
    df["latitude"] = latitude
    df["longitude"] = longitude
    df["load_date"] = datetime.utcnow().date()

    return df


def save_processed_parquet(df: pd.DataFrame, processed_path: str, city: str) -> str:
    os.makedirs(processed_path, exist_ok=True)

    load_date = df["load_date"].iloc[0]
    city_suffix = city.replace(" ", "_").lower()

    file_path = os.path.join(
        processed_path,
        f"weather_processed_{city_suffix}_{load_date}.parquet"
    )

    df.to_parquet(file_path, index=False)
    return file_path
