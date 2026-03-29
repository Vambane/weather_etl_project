import torch
import torch.nn as nn


class WeatherLSTM(nn.Module):
    """Stacked LSTM for multi-step weather forecasting.

    Input shape:  (batch, seq_len, num_features)
    Output shape: (batch, horizon, num_features)
    """

    def __init__(
        self,
        num_features: int = 3,
        hidden_size: int = 64,
        num_layers: int = 2,
        dropout: float = 0.2,
        horizon: int = 24,
    ):
        super().__init__()
        self.horizon = horizon
        self.num_features = num_features

        self.lstm = nn.LSTM(
            input_size=num_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            dropout=dropout if num_layers > 1 else 0.0,
            batch_first=True,
        )

        self.fc = nn.Linear(hidden_size, horizon * num_features)

    def forward(self, x):
        # x: (batch, seq_len, features)
        lstm_out, _ = self.lstm(x)
        # Use last timestep output
        last = lstm_out[:, -1, :]  # (batch, hidden_size)
        out = self.fc(last)  # (batch, horizon * features)
        return out.view(-1, self.horizon, self.num_features)
