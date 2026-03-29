import os
import joblib
import numpy as np
import pandas as pd
import torch
from datetime import timedelta

from etl.data_access import get_recent_hours
from forecast.dataset import FEATURES
from forecast.model import WeatherLSTM


def generate_forecast(
    city: str,
    conn,
    horizon: int = 24,
) -> pd.DataFrame:
    """Generate a weather forecast for a city using a trained LSTM model.

    Args:
        city: City name matching the trained model directory.
        conn: DuckDB connection.
        horizon: Forecast horizon in hours (24 or 168 for 7-day).

    Returns:
        DataFrame with predicted timestamp, temperature, humidity, precipitation.
    """
    horizon_label = "24h" if horizon <= 24 else "7d"
    model_dir = os.path.join("models", city.replace(" ", "_").lower())

    meta_path = os.path.join(model_dir, f"meta_{horizon_label}.pt")
    model_path = os.path.join(model_dir, f"lstm_{horizon_label}.pt")
    scaler_path = os.path.join(model_dir, f"scaler_{horizon_label}.joblib")

    if not os.path.exists(model_path):
        raise FileNotFoundError(f"No trained model found at {model_path}")

    meta = torch.load(meta_path, weights_only=True)
    scaler = joblib.load(scaler_path)

    lookback = meta["lookback"]

    device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")

    model = WeatherLSTM(
        num_features=meta["num_features"],
        hidden_size=meta["hidden_size"],
        num_layers=meta["num_layers"],
        dropout=meta["dropout"],
        horizon=meta["horizon"],
    ).to(device)
    model.load_state_dict(torch.load(model_path, map_location=device, weights_only=True))
    model.eval()

    # Get the most recent data for input
    df = get_recent_hours(conn, city, lookback)
    if len(df) < lookback:
        raise ValueError(
            f"Need {lookback} hours of data, only have {len(df)} for {city}"
        )

    values = df[FEATURES].values.astype(np.float32)
    scaled = scaler.transform(values)

    x = torch.FloatTensor(scaled).unsqueeze(0).to(device)

    with torch.no_grad():
        pred_scaled = model(x).cpu().numpy()[0]

    pred = scaler.inverse_transform(pred_scaled)

    last_timestamp = pd.Timestamp(df["timestamp"].iloc[-1])
    forecast_timestamps = [
        last_timestamp + timedelta(hours=i + 1) for i in range(horizon)
    ]

    forecast_df = pd.DataFrame(
        {
            "timestamp": forecast_timestamps,
            "temperature_2m": pred[:, 0],
            "relativehumidity_2m": np.clip(pred[:, 1], 0, 100),
            "precipitation": np.clip(pred[:, 2], 0, None),
            "city": city,
            "horizon_hours": horizon,
            "forecast_type": "lstm",
        }
    )

    return forecast_df
