"""Train LSTM weather forecasting models for all configured cities.

Usage:
    python train_models.py
"""

from datetime import datetime, timezone

from etl.config import load_config
from etl.load import connect_duckdb, create_weather_table
from etl.logger import get_logger
from forecast.train import train_model
from forecast.evaluate import evaluate_model

logger = get_logger()


def create_model_tables(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS weather_forecasts (
            city VARCHAR,
            forecast_timestamp TIMESTAMP,
            target_timestamp TIMESTAMP,
            horizon_hours INTEGER,
            temperature_2m DOUBLE,
            relativehumidity_2m DOUBLE,
            precipitation DOUBLE,
            model_version VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS model_metrics (
            city VARCHAR,
            horizon INTEGER,
            trained_at TIMESTAMP,
            mae_temp DOUBLE,
            rmse_temp DOUBLE,
            mae_humidity DOUBLE,
            rmse_humidity DOUBLE,
            mae_precip DOUBLE,
            rmse_precip DOUBLE
        )
        """
    )


def main():
    config = load_config()
    duckdb_path = config["paths"]["duckdb_path"]
    model_cfg = config.get("model", {})
    locations = config.get("locations", [])

    conn = connect_duckdb(duckdb_path)
    create_weather_table(conn)
    create_model_tables(conn)

    horizons = [
        (24, model_cfg.get("lookback_24h", 168)),
        (168, model_cfg.get("lookback_7d", 720)),
    ]

    for location in locations:
        city = location["name"]
        for horizon, lookback in horizons:
            label = "24h" if horizon <= 24 else "7d"
            logger.info(f"=== Training {label} model for {city} ===")
            try:
                train_model(
                    city=city,
                    conn=conn,
                    lookback=lookback,
                    horizon=horizon,
                    epochs=model_cfg.get("epochs", 50),
                    lr=model_cfg.get("learning_rate", 0.001),
                    batch_size=model_cfg.get("batch_size", 32),
                    hidden_size=model_cfg.get("hidden_size", 64),
                    num_layers=model_cfg.get("num_layers", 2),
                    dropout=model_cfg.get("dropout", 0.2),
                )

                metrics = evaluate_model(city, conn, lookback, horizon)
                if metrics:
                    conn.execute(
                        """
                        INSERT INTO model_metrics
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            city,
                            horizon,
                            datetime.now(timezone.utc),
                            metrics.get("mae_temperature_2m", 0),
                            metrics.get("rmse_temperature_2m", 0),
                            metrics.get("mae_relativehumidity_2m", 0),
                            metrics.get("rmse_relativehumidity_2m", 0),
                            metrics.get("mae_precipitation", 0),
                            metrics.get("rmse_precipitation", 0),
                        ],
                    )

                logger.info(f"=== {label} model for {city} complete ===")
            except Exception as e:
                logger.error(f"Failed to train {label} for {city}: {e}")

    logger.info("All model training complete.")


if __name__ == "__main__":
    main()
