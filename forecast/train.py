import os
import joblib
import torch
import torch.nn as nn
from torch.utils.data import DataLoader

from etl.logger import get_logger
from etl.data_access import get_weather_history
from forecast.dataset import prepare_datasets, FEATURES
from forecast.model import WeatherLSTM

logger = get_logger()


def train_model(
    city: str,
    conn,
    lookback: int = 168,
    horizon: int = 24,
    epochs: int = 50,
    lr: float = 0.001,
    batch_size: int = 32,
    hidden_size: int = 64,
    num_layers: int = 2,
    dropout: float = 0.2,
    patience: int = 7,
) -> str:
    """Train a WeatherLSTM model for a single city.

    Returns the directory path where model artifacts are saved.
    """
    logger.info(f"Training {horizon}h model for {city} (lookback={lookback})")

    # Fetch all available data
    df = conn.execute(
        """
        SELECT timestamp, temperature_2m, relativehumidity_2m, precipitation
        FROM weather_hourly
        WHERE city = ?
        ORDER BY timestamp
        """,
        [city],
    ).fetchdf()

    if len(df) < lookback + horizon + 100:
        raise ValueError(
            f"Not enough data for {city}: {len(df)} rows, "
            f"need at least {lookback + horizon + 100}"
        )

    logger.info(f"  Data rows: {len(df)}")

    train_ds, val_ds, test_ds, scaler = prepare_datasets(df, lookback, horizon)
    logger.info(
        f"  Splits: train={len(train_ds)}, val={len(val_ds)}, test={len(test_ds)}"
    )

    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=batch_size)

    device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")

    model = WeatherLSTM(
        num_features=len(FEATURES),
        hidden_size=hidden_size,
        num_layers=num_layers,
        dropout=dropout,
        horizon=horizon,
    ).to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = nn.MSELoss()

    best_val_loss = float("inf")
    epochs_no_improve = 0
    best_state = None

    for epoch in range(1, epochs + 1):
        # Train
        model.train()
        train_loss = 0.0
        for xb, yb in train_loader:
            xb, yb = xb.to(device), yb.to(device)
            pred = model(xb)
            loss = criterion(pred, yb)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            train_loss += loss.item() * len(xb)
        train_loss /= len(train_ds)

        # Validate
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for xb, yb in val_loader:
                xb, yb = xb.to(device), yb.to(device)
                pred = model(xb)
                val_loss += criterion(pred, yb).item() * len(xb)
        val_loss /= len(val_ds)

        if epoch % 5 == 0 or epoch == 1:
            logger.info(
                f"  Epoch {epoch}/{epochs} — train_loss={train_loss:.6f}, val_loss={val_loss:.6f}"
            )

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            epochs_no_improve = 0
            best_state = model.state_dict().copy()
        else:
            epochs_no_improve += 1
            if epochs_no_improve >= patience:
                logger.info(f"  Early stopping at epoch {epoch}")
                break

    # Save artifacts
    horizon_label = "24h" if horizon <= 24 else "7d"
    save_dir = os.path.join("models", city.replace(" ", "_").lower())
    os.makedirs(save_dir, exist_ok=True)

    model_path = os.path.join(save_dir, f"lstm_{horizon_label}.pt")
    scaler_path = os.path.join(save_dir, f"scaler_{horizon_label}.joblib")
    meta_path = os.path.join(save_dir, f"meta_{horizon_label}.pt")

    if best_state is not None:
        model.load_state_dict(best_state)

    torch.save(model.state_dict(), model_path)
    joblib.dump(scaler, scaler_path)
    torch.save(
        {
            "lookback": lookback,
            "horizon": horizon,
            "hidden_size": hidden_size,
            "num_layers": num_layers,
            "dropout": dropout,
            "num_features": len(FEATURES),
            "best_val_loss": best_val_loss,
        },
        meta_path,
    )

    logger.info(
        f"  Model saved to {model_path} (best val_loss={best_val_loss:.6f})"
    )
    return save_dir
