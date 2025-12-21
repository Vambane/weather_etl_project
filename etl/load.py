import duckdb
import pandas as pd # type: ignore
import os 
from typing import List, Dict

def connect_duckdb(db_path: str = "data/warehouse/weather.duckdb") -> duckdb.DuckDBPyConnection:

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
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

    # REMOVING DUPLICATES DATA
    conn.execute("""
                INSERT INTO weather_hourly (timestamp, temperature_2m, relativehumidity_2m, precipitation, city, latitude, longitude, load_date)
                SELECT timestamp, temperature_2m, relativehumidity_2m, precipitation, city, latitude, longitude, load_date
                FROM df
                WHERE (timestamp, latitude, longitude, city) NOT IN (
                    SELECT timestamp, latitude, longitude, city
                    FROM weather_hourly
        );
                 """)
    # INSERTING NEW DATA
    # conn.execute("INSERT INTO weather_hourly SELECT * FROM df")


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
