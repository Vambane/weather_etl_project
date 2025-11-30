import duckdb
import pandas as pd # type: ignore
import os 

def connect_duckdb(db_path: str = "data/warehouse/weather.duckdb") -> duckdb.DuckDBPyConnection:

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return duckdb.connect(db_path)

def create_weather_table(conn: duckdb.DuckDBPyConnection):

    query = """
    CREATE TABLE IF NOT EXISTS weather_hourly (
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

def upsert_weather_data(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame):

    # CREATE TABLE
    create_weather_table(conn)

    # REGISTER the pandas dataframe  as DuckDB table
    conn.register("df", df)

    # REMOVING DUPLICATES DATA
    conn.execute("""
                INSERT INTO weather_hourly
                SELECT *
                FROM df
                WHERE (timestamp, latitude, longitude) NOT IN (
                    SELECT timestamp, latitude, longitude
                    FROM weather_hourly
        );
                 """)
    # INSERTING NEW DATA
    conn.execute("INSERT INTO weather_hourly SELECT * FROM df")


    print("Upsert completed. Data loaded")


