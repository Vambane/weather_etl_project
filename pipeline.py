from etl.extract import extract_weather_data
from etl.transform import transform_weather_data, load_raw_json
from etl.load import connect_duckdb, upsert_weather_data, backfill_city
from etl.logger import get_logger
from etl.config import load_config
from etl.transform import save_processed_parquet

import time
import traceback

logger = get_logger()

def run_pipeline(): 
    config = load_config()

    locations = config.get("locations") or ([config["location"]] if "location" in config else [])
    if not locations:
        raise ValueError("No locations defined in config.yaml")

    raw_path = config["paths"]["raw_path"]
    duckdb_path = config["paths"]["duckdb_path"]

    print("=== WEATHER ETL PIPELINE STARTED ===")
    start_time = time.time()

    try:
        conn = connect_duckdb(duckdb_path)

        total_rows = 0
        for location in locations:
            city = location.get("name", "unknown")
            latitude = location["latitude"]
            longitude = location["longitude"]

            logger.info(f"--- Processing location: {city} ({latitude}, {longitude}) ---")

            # -----------------------------
            # EXTRACT
            # -----------------------------
            t0 = time.time()
            raw_file = extract_weather_data(
                latitude=latitude,
                longitude=longitude,
                raw_path=raw_path,
                city=city
            )

            logger.info(f"Extract step completed. Raw file: {raw_file}")
            logger.info(f"Extract step duration: {time.time() - t0:.3f} seconds")

            # -----------------------------
            # TRANSFORM
            # -----------------------------
            t1 = time.time()
            raw_json = load_raw_json(raw_file)
            df = transform_weather_data(
                raw_json=raw_json,
                latitude=latitude,
                longitude=longitude,
                city=city
            )

            # Save processed parquet
            processed_path = config["paths"]["processed_path"]
            parquet_file = save_processed_parquet(df, processed_path, city)
            logger.info(f"Processed data saved to: {parquet_file}")
            logger.info(f"Transform step completed. Records transformed: {len(df)}")
            logger.info(f"Transform step duration: {time.time() - t1:.3f} seconds")

            # -----------------------------
            # LOAD
            # -----------------------------
            t2 = time.time()
            upsert_weather_data(conn, df)
            total_rows += len(df)
            logger.info(f"Load step completed for {city}. Rows inserted: {len(df)}")
            logger.info(f"Load step duration: {time.time() - t2:.3f} seconds")

        # Backfill city for any existing nulls (e.g., older ingested rows)
        backfill_city(conn, locations)

        # -----------------------------
        # TOTAL RUNTIME
        # -----------------------------
        total_runtime = time.time() - start_time
        logger.info(f"=== WEATHER ETL PIPELINE COMPLETED in {total_runtime:.3f} seconds ===")
        print(f"Loaded {total_rows} records into DuckDB.")
    except Exception as e:
        # Lof the error and stack trace
        logger.error("PIPELINE FAILED")
        logger.error(f"Error: {str(e)}")
        logger.error(traceback.format_exc())

        # Log failure runtime
        total_runtime = time.time() - start_time
        logger.info(f"=== WEATHER ETL PIPELINE FAILED after {total_runtime:.3f} seconds ===")

    finally:
        print("=== WEATHER ETL PIPELINE ENDED (SUCCESS OR FAILURE)===")

if __name__ == "__main__":
    # Johannesburg, South Africa
    run_pipeline()
