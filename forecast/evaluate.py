import numpy as np
import torch
from torch.utils.data import DataLoader
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from etl.logger import get_logger
from forecast.dataset import prepare_datasets, FEATURES
from forecast.model import WeatherLSTM

logger = get_logger()


def evaluate_model(city: str, conn, lookback: int = 168, horizon: int = 24) -> dict:
    """Evaluate a trained model on the test split and return metrics per feature.

    Returns:
        Dict with keys like mae_temperature_2m, rmse_temperature_2m, r2_temperature_2m, etc.
    """
    import os
    import joblib

    horizon_label = "24h" if horizon <= 24 else "7d"
    model_dir = os.path.join("models", city.replace(" ", "_").lower())
    meta = torch.load(
        os.path.join(model_dir, f"meta_{horizon_label}.pt"), weights_only=True
    )
    scaler = joblib.load(os.path.join(model_dir, f"scaler_{horizon_label}.joblib"))

    device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
    model = WeatherLSTM(
        num_features=meta["num_features"],
        hidden_size=meta["hidden_size"],
        num_layers=meta["num_layers"],
        dropout=meta["dropout"],
        horizon=meta["horizon"],
    ).to(device)
    model.load_state_dict(
        torch.load(
            os.path.join(model_dir, f"lstm_{horizon_label}.pt"),
            map_location=device,
            weights_only=True,
        )
    )
    model.eval()

    df = conn.execute(
        """
        SELECT timestamp, temperature_2m, relativehumidity_2m, precipitation
        FROM weather_hourly WHERE city = ? ORDER BY timestamp
        """,
        [city],
    ).fetchdf()

    _, _, test_ds, _ = prepare_datasets(df, lookback, horizon)

    if len(test_ds) == 0:
        logger.warning(f"No test data for {city}")
        return {}

    test_loader = DataLoader(test_ds, batch_size=64)

    all_preds = []
    all_targets = []
    with torch.no_grad():
        for xb, yb in test_loader:
            xb = xb.to(device)
            pred = model(xb).cpu().numpy()
            all_preds.append(pred)
            all_targets.append(yb.numpy())

    preds = np.concatenate(all_preds)
    targets = np.concatenate(all_targets)

    # Inverse-transform for metrics in original scale
    n_samples = preds.shape[0] * preds.shape[1]
    preds_flat = scaler.inverse_transform(preds.reshape(-1, len(FEATURES)))
    targets_flat = scaler.inverse_transform(targets.reshape(-1, len(FEATURES)))

    metrics = {"city": city, "horizon": horizon}
    for i, feat in enumerate(FEATURES):
        p = preds_flat[:, i]
        t = targets_flat[:, i]
        metrics[f"mae_{feat}"] = float(mean_absolute_error(t, p))
        metrics[f"rmse_{feat}"] = float(np.sqrt(mean_squared_error(t, p)))
        metrics[f"r2_{feat}"] = float(r2_score(t, p))

    logger.info(f"Evaluation for {city} ({horizon_label}):")
    for feat in FEATURES:
        logger.info(
            f"  {feat}: MAE={metrics[f'mae_{feat}']:.3f}, "
            f"RMSE={metrics[f'rmse_{feat}']:.3f}, "
            f"R2={metrics[f'r2_{feat}']:.3f}"
        )

    return metrics
