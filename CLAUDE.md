# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

A weather forecasting platform that combines an ETL pipeline, LSTM deep learning models, and a Streamlit dashboard. Fetches hourly weather data from [Open-Meteo](https://open-meteo.com), validates and stores it in DuckDB, trains PyTorch LSTM models for 24-hour and 7-day forecasts, and presents everything through an interactive web dashboard.

## Quick Start

```bash
pip install -r requirements.txt

# Run ETL pipeline (fetches fresh data + generates forecasts)
python pipeline.py

# Backfill historical data for model training
python backfill.py --start 2024-01-01 --end 2026-03-20

# Train LSTM models
python train_models.py

# Launch dashboard
streamlit run app.py

# Or do pipeline + dashboard in one step
./run.sh
```

Requires Python 3.10+.

## Architecture

### ETL Pipeline (`etl/`)
- `extract.py` — Calls Open-Meteo API with retry logic, saves raw JSON to `data/raw/`
- `transform.py` — Validates data, runs DQC checks (range, freshness, completeness), outputs pandas DataFrame
- `load.py` — Upserts into DuckDB `weather_hourly` table, deduplicates on `(timestamp, latitude, longitude, city)`
- `data_access.py` — Shared query functions for history, latest weather, city list, freshness
- `config.py` — Loads `config.yaml`
- `logger.py` — File + console logging to `logs/<date>.log`

### Forecast (`forecast/`)
- `dataset.py` — PyTorch Dataset with sliding windows, train/val/test splits, MinMaxScaler
- `model.py` — `WeatherLSTM`: 2-layer stacked LSTM with dropout
- `train.py` — Training loop with Adam, MSE loss, early stopping
- `predict.py` — Loads saved model, runs inference, returns forecast DataFrame
- `evaluate.py` — Computes MAE, RMSE, R² per feature on test split

### Dashboard (`dashboard/`)
- `components.py` — Streamlit UI components (metric cards, city selector, horizon toggle)
- `charts.py` — Plotly chart builders (temperature forecast, humidity, precipitation, multi-city, model performance)
- `data.py` — Cached data fetching layer wrapping `data_access.py`

### Entry Points
- `pipeline.py` — Main ETL orchestrator, also generates forecasts after loading
- `backfill.py` — Fetches historical data from Open-Meteo archive API in 90-day chunks
- `train_models.py` — Trains 24h and 7d LSTM models for all configured cities
- `app.py` — Streamlit dashboard entry point

## Running Tests

```bash
python -m pytest tests/ -v
```

## Configuration

All settings in `config.yaml`:
- `locations` — List of cities with lat/lon coordinates
- `settings.hours_to_fetch` — Expected hourly records per API call (168 = 7 days)
- `settings.dqc_enabled` — Toggle data quality checks
- `settings.backfill_start_date` — Default start date for historical backfill
- `model.*` — LSTM hyperparameters (lookback, epochs, batch_size, learning_rate, etc.)
- `dashboard.*` — Dashboard port and theme

## Data Storage

- Raw JSON: `data/raw/`
- Processed Parquet: `data/processed/`
- DuckDB warehouse: `data/warehouse/weather.duckdb`
- Model checkpoints: `models/<city>/lstm_24h.pt`, `lstm_7d.pt`
- Logs: `logs/<date>.log`

## DuckDB Tables

- `weather_hourly` — Historical + current hourly weather data
- `weather_forecasts` — Stored LSTM forecast predictions
- `model_metrics` — Training evaluation metrics (MAE, RMSE per feature)

## Key Patterns

- Per-city try/except in pipeline so one city failing doesn't stop others
- `datetime.now(timezone.utc)` used throughout (no deprecated `utcnow()`)
- DuckDB locking: only one process can hold the `.duckdb` file at a time
- Forecast models are per-city with separate 24h and 7d checkpoints
- Streamlit caching with TTL for responsive dashboard performance
