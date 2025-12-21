# Weather ETL Pipeline

Python ETL that fetches 7-day hourly weather forecasts from Open-Meteo, applies data quality checks, and loads the results into a DuckDB warehouse. The pipeline supports multiple cities (e.g., Johannesburg and Cape Town) and includes a notebook for side-by-side comparisons.

## Project layout
- `pipeline.py` — orchestrates extract, transform, load, and city backfill
- `etl/extract.py` — calls the Open-Meteo API and saves raw JSON
- `etl/transform.py` — validates and shapes hourly data into a clean DataFrame
- `etl/load.py` — creates/updates the `weather_hourly` table in DuckDB and deduplicates
- `config.yaml` — locations, paths, and settings
- `data/` — raw JSON, processed Parquet, and the DuckDB file (`warehouse/weather.duckdb`)
- `notebooks/weather_analysis.ipynb` — visual comparisons across cities
- `logs/` — runtime logs

## Data model
Table `weather_hourly` in DuckDB:
- `city` (VARCHAR)
- `timestamp` (TIMESTAMP)
- `temperature_2m` (DOUBLE)
- `relativehumidity_2m` (DOUBLE)
- `precipitation` (DOUBLE)
- `latitude` (DOUBLE)
- `longitude` (DOUBLE)
- `load_date` (DATE)

Primary uniqueness is enforced during load on `(timestamp, latitude, longitude, city)`.

## Configuration
`config.yaml` drives the run:
```yaml
locations:
  - name: Johannesburg
    latitude: -26.2041
    longitude: 28.0473
  - name: Cape Town
    latitude: -33.9249
    longitude: 18.4241

paths:
  raw_path: "data/raw"
  processed_path: "data/processed"
  duckdb_path: "data/warehouse/weather.duckdb"

settings:
  hours_to_fetch: 168
  dqc_enabled: true
```
Add or remove cities by editing the `locations` list. Paths are workspace-relative.

## Setup
1) Python 3.10+ recommended.  
2) Install dependencies (one-time):
```bash
pip install duckdb pandas requests pyarrow matplotlib
```
3) Ensure `data/` exists; it is created automatically on first run.

## Running the pipeline
From the repo root:
```bash
python pipeline.py
```
What happens:
- For each configured city, extract hourly forecast JSON to `data/raw`.
- Transform with data quality checks (presence, ranges, freshness, hourly completeness).
- Save processed Parquet per city to `data/processed/weather_processed_<city>_<date>.parquet`.
- Upsert into DuckDB `weather_hourly`, deduping on timestamp/lat/lon/city.
- Backfill missing `city` values in older rows by matching lat/lon.

If DuckDB reports a lock, close other processes using `data/warehouse/weather.duckdb` and rerun.

## Exploring the data
1) Open the notebook: `notebooks/weather_analysis.ipynb`.  
2) Run cells to:
   - Load `weather_hourly` for all cities
   - Compare temperature, humidity, and precipitation across cities
   - View daily aggregates and temperature deltas between cities

## Logs and outputs
- Raw JSON: `data/raw/weather_raw_<city>_<timestamp>.json`
- Processed Parquet: `data/processed/weather_processed_<city>_<date>.parquet`
- Warehouse: `data/warehouse/weather.duckdb`
- Logs: `logs/` (pipeline events, timings, errors)

## Maintenance notes
- To add cities, update `config.yaml` and rerun the pipeline.
- To clear data, remove or archive files under `data/` (ensure no other process holds the DuckDB lock).
- Data quality checks can be extended in `etl/transform.py`; keep the expected record count in sync with `settings.hours_to_fetch`.
