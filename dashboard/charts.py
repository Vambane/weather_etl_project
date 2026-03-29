import plotly.graph_objects as go
import plotly.express as px
import pandas as pd


COLOR_ACTUAL = "#2196F3"
COLOR_FORECAST = "#FF9800"
COLOR_BAND = "rgba(255, 152, 0, 0.15)"


def plot_temperature_forecast(
    actual_df: pd.DataFrame, forecast_df: pd.DataFrame, city: str
) -> go.Figure:
    fig = go.Figure()

    if not actual_df.empty:
        fig.add_trace(
            go.Scatter(
                x=actual_df["timestamp"],
                y=actual_df["temperature_2m"],
                name="Actual",
                line=dict(color=COLOR_ACTUAL, width=2),
            )
        )

    if not forecast_df.empty:
        # Bridge the visual gap: prepend the last actual point so the forecast
        # line connects seamlessly to the actual line (forecast starts at
        # last_actual + 1h, leaving a one-step break without this fix).
        plot_df = forecast_df[["timestamp", "temperature_2m"]].copy()
        if not actual_df.empty:
            last_actual_ts = actual_df["timestamp"].iloc[-1]
            first_forecast_ts = plot_df["timestamp"].iloc[0]
            if first_forecast_ts > last_actual_ts:
                bridge = pd.DataFrame(
                    {
                        "timestamp": [last_actual_ts],
                        "temperature_2m": [actual_df["temperature_2m"].iloc[-1]],
                    }
                )
                plot_df = pd.concat([bridge, plot_df], ignore_index=True)

        # Confidence band (simple +/- based on typical MAE)
        upper = plot_df["temperature_2m"] + 5
        lower = plot_df["temperature_2m"] - 5

        fig.add_trace(
            go.Scatter(
                x=plot_df["timestamp"],
                y=upper,
                mode="lines",
                line=dict(width=0),
                showlegend=False,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=plot_df["timestamp"],
                y=lower,
                mode="lines",
                line=dict(width=0),
                fill="tonexty",
                fillcolor=COLOR_BAND,
                showlegend=False,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=plot_df["timestamp"],
                y=plot_df["temperature_2m"],
                name="Forecast",
                line=dict(color=COLOR_FORECAST, width=2, dash="dash"),
            )
        )

    fig.update_layout(
        title=f"Temperature - {city}",
        xaxis_title="Time",
        yaxis_title="Temperature (C)",
        template="plotly_white",
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def plot_humidity_chart(df: pd.DataFrame, city: str) -> go.Figure:
    fig = go.Figure()
    if not df.empty:
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["relativehumidity_2m"],
                name="Humidity",
                line=dict(color="#4CAF50", width=2),
                fill="tozeroy",
                fillcolor="rgba(76, 175, 80, 0.1)",
            )
        )
    fig.update_layout(
        title=f"Humidity - {city}",
        xaxis_title="Time",
        yaxis_title="Humidity (%)",
        template="plotly_white",
        height=350,
    )
    return fig


def plot_precipitation_chart(df: pd.DataFrame, city: str) -> go.Figure:
    fig = go.Figure()
    if not df.empty:
        fig.add_trace(
            go.Bar(
                x=df["timestamp"],
                y=df["precipitation"],
                name="Precipitation",
                marker_color="#5C6BC0",
            )
        )
    fig.update_layout(
        title=f"Precipitation - {city}",
        xaxis_title="Time",
        yaxis_title="Precipitation (mm)",
        template="plotly_white",
        height=350,
    )
    return fig


def plot_multi_city_comparison(df: pd.DataFrame, metric: str) -> go.Figure:
    labels = {
        "temperature_2m": "Temperature (C)",
        "relativehumidity_2m": "Humidity (%)",
        "precipitation": "Precipitation (mm)",
    }
    fig = px.line(
        df,
        x="timestamp",
        y=metric,
        color="city",
        title=f"Multi-City Comparison: {labels.get(metric, metric)}",
        template="plotly_white",
        height=400,
    )
    fig.update_layout(
        yaxis_title=labels.get(metric, metric),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def plot_model_performance(metrics_df: pd.DataFrame) -> go.Figure:
    if metrics_df.empty:
        fig = go.Figure()
        fig.update_layout(title="No model metrics available")
        return fig

    fig = go.Figure()
    for _, row in metrics_df.iterrows():
        label = f"{row['city']} ({row['horizon']}h)"
        fig.add_trace(
            go.Bar(
                name=f"{label} MAE",
                x=["Temperature", "Humidity", "Precipitation"],
                y=[row["mae_temp"], row["mae_humidity"], row["mae_precip"]],
            )
        )

    fig.update_layout(
        title="Model Performance (MAE by Feature)",
        barmode="group",
        template="plotly_white",
        height=400,
        yaxis_title="Mean Absolute Error",
    )
    return fig
