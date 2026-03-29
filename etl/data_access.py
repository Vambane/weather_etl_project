import duckdb
import pandas as pd
from typing import List, Optional


def get_weather_history(
    conn: duckdb.DuckDBPyConnection,
    city: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT timestamp, temperature_2m, relativehumidity_2m, precipitation,
               city, latitude, longitude
        FROM weather_hourly
        WHERE city = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp
        """,
        [city, start_date, end_date],
    ).fetchdf()


def get_latest_weather(
    conn: duckdb.DuckDBPyConnection, city: str
) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT timestamp, temperature_2m, relativehumidity_2m, precipitation,
               city, latitude, longitude
        FROM weather_hourly
        WHERE city = ?
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        [city],
    ).fetchdf()


def get_available_cities(conn: duckdb.DuckDBPyConnection) -> List[str]:
    rows = conn.execute(
        "SELECT DISTINCT city FROM weather_hourly WHERE city IS NOT NULL ORDER BY city"
    ).fetchall()
    return [row[0] for row in rows]


def get_data_freshness(conn: duckdb.DuckDBPyConnection) -> dict:
    rows = conn.execute(
        """
        SELECT city, MAX(timestamp) AS latest_timestamp, MAX(load_date) AS last_load
        FROM weather_hourly
        WHERE city IS NOT NULL
        GROUP BY city
        ORDER BY city
        """
    ).fetchdf()
    result = {}
    for _, row in rows.iterrows():
        result[row["city"]] = {
            "latest_timestamp": row["latest_timestamp"],
            "last_load": row["last_load"],
        }
    return result


def get_recent_hours(
    conn: duckdb.DuckDBPyConnection, city: str, hours: int
) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT timestamp, temperature_2m, relativehumidity_2m, precipitation
        FROM weather_hourly
        WHERE city = ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        [city, hours],
    ).fetchdf().sort_values("timestamp").reset_index(drop=True)
