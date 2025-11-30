from etl.extract import extract_weather_data
from etl.transform import transform_weather_data, load_raw_json
from etl.load import connect_duckdb, upsert_weather_data

def run_pipeline(latitude: float, longitude: float): 
    print("=== WEATHER ETL PIPELINE STARTED ===")

    # EXTRACTING THE DATA
    raw_file = extract_weather_data(
        latitude=latitude,
        longitude=longitude,
        raw_path="data/raw"
    )

    # TRANSFORMING THE DATA
    raw_json = load_raw_json(raw_file)
    df = transform_weather_data(
        raw_json=raw_json,
        latitude=latitude,
        longitude=longitude
    )

    # LOADING THE DATA
    conn = connect_duckdb()
    upsert_weather_data(conn, df)

    print("=== WEATHER ETL PIPELINE COMPLETED ===")
    print(f"Loaded {len(df)} records into DuckDB.")

if __name__ == "__main__":
    # Johannesburg, South Africa
    run_pipeline(latitude=-26.2041, longitude=28.0473)
