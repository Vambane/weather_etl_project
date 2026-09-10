import duckdb
import pandas as pd # type: ignore
import os 
from typing import List, Dict

def connect_duckdb(db_path: str = "data/warehouse/weather.duckdb") -> duckdb.DuckDBPyConnection:

    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    return duckdb.connect(db_path)

def create_weather_table(conn: duckdb.DuckDBPyConnection):

    query = """
    CREATE TABLE IF NOT EXISTS weather_hourly (
        city VARCHAR,
        timestamp TIMESTAMP,
        temperature_2m DOUBLE,
        relativehumidity_2m DOUBLE,
        precipitation DOUBLE,
        latitude DOUBLE,
        longitude DOUBLE,
        load_date DATE
        ); 
        """
    conn.execute(query)
    columns = {row[1] for row in conn.execute("PRAGMA table_info('weather_hourly')").fetchall()}
    if "city" not in columns:
        conn.execute("ALTER TABLE weather_hourly ADD COLUMN city VARCHAR")

def upsert_weather_data(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame):

    # CREATE TABLE
    create_weather_table(conn)

    # REGISTER the pandas dataframe  as DuckDB table
    conn.register("df", df)

    conn.execute("""
        DELETE FROM weather_hourly target
        USING df source
        WHERE target.timestamp = source.timestamp
          AND target.latitude = source.latitude
          AND target.longitude = source.longitude
          AND target.city IS NOT DISTINCT FROM source.city
    """)
    conn.execute("""
        INSERT INTO weather_hourly
            (city, timestamp, temperature_2m, relativehumidity_2m, precipitation,
             latitude, longitude, load_date)
        SELECT city, timestamp, temperature_2m, relativehumidity_2m, precipitation,
               latitude, longitude, load_date
        FROM df
    """)


    print("Upsert completed. Data loaded")


def backfill_city(conn: duckdb.DuckDBPyConnection, locations: List[Dict]):
    """
    Backfill missing city values based on exact latitude/longitude matches.
    """
    for loc in locations:
        city = loc.get("name")
        lat = loc.get("latitude")
        lon = loc.get("longitude")
        if city is None or lat is None or lon is None:
            continue
        conn.execute(
            """
            UPDATE weather_hourly
            SET city = ?
            WHERE city IS NULL AND latitude = ? AND longitude = ?
            """,
            [city, lat, lon]
        )
