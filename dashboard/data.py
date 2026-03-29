import streamlit as st
import duckdb
import pandas as pd

from etl.data_access import (
    get_available_cities,
    get_data_freshness,
    get_latest_weather,
    get_weather_history,
)
from forecast.predict import generate_forecast


@st.cache_resource
def get_connection(db_path: str = "data/warehouse/weather.duckdb"):
    return duckdb.connect(db_path, read_only=True)


@st.cache_data(ttl=300)
def fetch_cities(_conn) -> list[str]:
    return get_available_cities(_conn)


@st.cache_data(ttl=300)
def fetch_freshness(_conn) -> dict:
    return get_data_freshness(_conn)


@st.cache_data(ttl=300)
def fetch_latest(_conn, city: str) -> pd.DataFrame:
    return get_latest_weather(_conn, city)


@st.cache_data(ttl=300)
def fetch_history(
    _conn, city: str, start_date: str, end_date: str
) -> pd.DataFrame:
    return get_weather_history(_conn, city, start_date, end_date)


@st.cache_data(ttl=600)
def fetch_forecast(_conn, city: str, horizon: int) -> pd.DataFrame:
    try:
        return generate_forecast(city, _conn, horizon)
    except (FileNotFoundError, ValueError) as e:
        st.warning(f"Forecast not available for {city}: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_model_metrics(_conn) -> pd.DataFrame:
    try:
        return _conn.execute("SELECT * FROM model_metrics ORDER BY city, horizon").fetchdf()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_multi_city_data(_conn, cities: list[str], days: int = 7) -> pd.DataFrame:
    if not cities:
        return pd.DataFrame()
    placeholders = ", ".join(["?"] * len(cities))
    return _conn.execute(
        f"""
        SELECT timestamp, temperature_2m, relativehumidity_2m, precipitation, city
        FROM weather_hourly
        WHERE city IN ({placeholders})
          AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '{days} days'
        ORDER BY timestamp
        """,
        cities,
    ).fetchdf()
