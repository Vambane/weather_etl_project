"""Backfill historical weather data from Open-Meteo archive API.

Usage:
    python backfill.py
    python backfill.py --start 2024-06-01 --end 2024-12-31

Fetches historical hourly data in 90-day chunks to stay within API limits,
then loads into DuckDB using the standard transform/load pipeline.
"""

import argparse
from datetime import datetime, timedelta, timezone

from etl.config import load_config
from etl.extract import extract_historical_data
from etl.transform import transform_weather_data, load_raw_json
from etl.load import connect_duckdb, upsert_weather_data
from etl.logger import get_logger

logger = get_logger()

CHUNK_DAYS = 90


def backfill(start_date: str, end_date: str):
    config = load_config()
    locations = config.get("locations", [])
    raw_path = config["paths"]["raw_path"]
    duckdb_path = config["paths"]["duckdb_path"]

    conn = connect_duckdb(duckdb_path)

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    for location in locations:
        city = location["name"]
        lat = location["latitude"]
        lon = location["longitude"]
        logger.info(f"=== Backfilling {city} from {start_date} to {end_date} ===")

        chunk_start = start
        total_rows = 0

        while chunk_start < end:
            chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS - 1), end)
            s = chunk_start.strftime("%Y-%m-%d")
            e = chunk_end.strftime("%Y-%m-%d")

            logger.info(f"  Fetching {city}: {s} to {e}")

            try:
                raw_file = extract_historical_data(
                    latitude=lat,
                    longitude=lon,
                    start_date=s,
                    end_date=e,
                    raw_path=raw_path,
                    city=city,
                )

                raw_json = load_raw_json(raw_file)
                df = transform_weather_data(
                    raw_json=raw_json,
                    latitude=lat,
                    longitude=lon,
                    city=city,
                    dqc_enabled=True,
                    is_historical=True,
                )

                upsert_weather_data(conn, df)
                total_rows += len(df)
                logger.info(f"  Loaded {len(df)} rows for {city} ({s} to {e})")

            except Exception as ex:
                logger.error(f"  Failed chunk {s} to {e} for {city}: {ex}")

            chunk_start = chunk_end + timedelta(days=1)

        logger.info(f"=== {city} backfill complete: {total_rows} total rows ===")

    logger.info("Backfill finished for all cities.")


if __name__ == "__main__":
    config = load_config()
    default_start = config.get("settings", {}).get("backfill_start_date", "2024-01-01")
    default_end = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(description="Backfill historical weather data")
    parser.add_argument("--start", default=default_start, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=default_end, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    backfill(args.start, args.end)
