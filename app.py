import streamlit as st
from datetime import datetime, timedelta, timezone

from dashboard.data import (
    get_connection,
    fetch_cities,
    fetch_freshness,
    fetch_latest,
    fetch_history,
    fetch_forecast,
    fetch_model_metrics,
    fetch_multi_city_data,
)
from dashboard.components import render_city_selector, render_horizon_toggle, render_metric_cards
from dashboard.charts import (
    plot_temperature_forecast,
    plot_humidity_chart,
    plot_precipitation_chart,
    plot_multi_city_comparison,
    plot_model_performance,
)

st.set_page_config(
    page_title="Weather Forecast Platform",
    page_icon="☁",
    layout="wide",
)

st.title("Weather Forecast Platform")
st.caption("Real-time weather data + LSTM-powered forecasts")

conn = get_connection()

# Sidebar
st.sidebar.header("Controls")
cities = fetch_cities(conn)
if not cities:
    st.error("No weather data found. Run the pipeline first: `python pipeline.py`")
    st.stop()

city = render_city_selector(cities)
horizon = render_horizon_toggle()

if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.divider()
st.sidebar.markdown("**Data Sources**")
st.sidebar.markdown("Weather: [Open-Meteo](https://open-meteo.com)")
st.sidebar.markdown("Forecasts: LSTM (PyTorch)")

# Row 1: Metric cards
freshness = fetch_freshness(conn)
current = fetch_latest(conn, city)
render_metric_cards(current, freshness, city)

st.divider()

# Row 2: Temperature chart with forecast
now = datetime.now(timezone.utc)
start = (now - timedelta(days=7)).strftime("%Y-%m-%d")
end = now.strftime("%Y-%m-%d %H:%M:%S")

actual_df = fetch_history(conn, city, start, end)
forecast_df = fetch_forecast(conn, city, horizon)

st.plotly_chart(
    plot_temperature_forecast(actual_df, forecast_df, city),
    use_container_width=True,
)

# Row 3: Humidity & Precipitation side by side
col1, col2 = st.columns(2)
with col1:
    st.plotly_chart(plot_humidity_chart(actual_df, city), use_container_width=True)
with col2:
    st.plotly_chart(plot_precipitation_chart(actual_df, city), use_container_width=True)

st.divider()

# Row 4: Multi-city comparison
st.subheader("Multi-City Comparison")
metric = st.selectbox(
    "Metric",
    ["temperature_2m", "relativehumidity_2m", "precipitation"],
    format_func=lambda x: {
        "temperature_2m": "Temperature",
        "relativehumidity_2m": "Humidity",
        "precipitation": "Precipitation",
    }[x],
)
multi_df = fetch_multi_city_data(conn, cities)
if not multi_df.empty:
    st.plotly_chart(
        plot_multi_city_comparison(multi_df, metric), use_container_width=True
    )
else:
    st.info("Not enough multi-city data for comparison.")

st.divider()

# Row 5: Model performance
st.subheader("Model Performance")
metrics_df = fetch_model_metrics(conn)
st.plotly_chart(plot_model_performance(metrics_df), use_container_width=True)
