import os 
import json
import requests
from datetime import datetime, timezone
from math import ceil
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional


HOURLY_FIELDS = "temperature_2m,relativehumidity_2m,precipitation"


def build_weather_url(latitude: float, longitude: float, historical: bool = False) -> str:
    return "https://archive-api.open-meteo.com/v1/archive" if historical else "https://api.open-meteo.com/v1/forecast"


def _request_weather(url: str, params: dict) -> dict:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def _save_raw(data: dict, raw_path: str, city: Optional[str]) -> str:
    os.makedirs(raw_path, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    city_suffix = f"{city.replace(' ', '_').lower()}_" if city else ""
    file_path = os.path.join(raw_path, f"weather_raw_{city_suffix}{timestamp}.json")
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)
    return file_path

def extract_weather_data(
    latitude: float,
    longitude: float,
    raw_path: str,
    city: Optional[str] = None,
    forecast_days: int = 7,
) -> str:
    url = build_weather_url(latitude, longitude)
    print(f"Requesting Weather Data from: {url}")
    data = _request_weather(url, {
        "latitude": latitude, "longitude": longitude, "hourly": HOURLY_FIELDS,
        "timezone": "UTC", "forecast_days": forecast_days,
    })
    file_path = _save_raw(data, raw_path, city)
    print(f"Raw Data save to: {file_path}")
    return file_path


def extract_historical_data(
    latitude: float, longitude: float, start_date: str, end_date: str,
    raw_path: str, city: Optional[str] = None,
) -> str:
    url = build_weather_url(latitude, longitude, historical=True)
    data = _request_weather(url, {
        "latitude": latitude, "longitude": longitude, "hourly": HOURLY_FIELDS,
        "timezone": "UTC", "start_date": start_date, "end_date": end_date,
    })
    return _save_raw(data, raw_path, city)
