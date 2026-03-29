import numpy as np
import torch
from torch.utils.data import Dataset
from sklearn.preprocessing import MinMaxScaler


FEATURES = ["temperature_2m", "relativehumidity_2m", "precipitation"]


class WeatherSequenceDataset(Dataset):
    """Sliding-window dataset for weather time series.

    Creates (input, target) pairs from a contiguous block of weather data.
    Input: lookback hours of 3 features.
    Target: horizon hours of 3 features.
    """

    def __init__(self, data: np.ndarray, lookback: int, horizon: int):
        self.lookback = lookback
        self.horizon = horizon
        self.data = data
        self.samples = len(data) - lookback - horizon + 1

    def __len__(self):
        return max(0, self.samples)

    def __getitem__(self, idx):
        x = self.data[idx : idx + self.lookback]
        y = self.data[idx + self.lookback : idx + self.lookback + self.horizon]
        return torch.FloatTensor(x), torch.FloatTensor(y)


def prepare_datasets(df, lookback, horizon, train_frac=0.7, val_frac=0.15):
    """Split a DataFrame into train/val/test datasets with fitted scaler.

    Args:
        df: DataFrame with columns matching FEATURES.
        lookback: Number of past hours used as input.
        horizon: Number of future hours to predict.
        train_frac: Fraction of data for training.
        val_frac: Fraction of data for validation. Test gets the remainder.

    Returns:
        Tuple of (train_dataset, val_dataset, test_dataset, scaler).
    """
    values = df[FEATURES].values.astype(np.float32)

    n = len(values)
    train_end = int(n * train_frac)
    val_end = int(n * (train_frac + val_frac))

    train_raw = values[:train_end]
    val_raw = values[train_end:val_end]
    test_raw = values[val_end:]

    scaler = MinMaxScaler()
    train_scaled = scaler.fit_transform(train_raw)
    val_scaled = scaler.transform(val_raw)
    test_scaled = scaler.transform(test_raw)

    train_ds = WeatherSequenceDataset(train_scaled, lookback, horizon)
    val_ds = WeatherSequenceDataset(val_scaled, lookback, horizon)
    test_ds = WeatherSequenceDataset(test_scaled, lookback, horizon)

    return train_ds, val_ds, test_ds, scaler
