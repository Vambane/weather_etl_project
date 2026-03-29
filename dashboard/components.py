import streamlit as st
import pandas as pd
from datetime import datetime, timezone


def render_city_selector(cities: list[str]) -> str:
    return st.sidebar.selectbox("City", cities)


def render_horizon_toggle() -> int:
    choice = st.sidebar.radio("Forecast Horizon", ["24 Hours", "7 Days"])
    return 24 if choice == "24 Hours" else 168


def render_metric_cards(current_data: pd.DataFrame, freshness: dict, city: str):
    if current_data.empty:
        st.warning("No current weather data available.")
        return

    row = current_data.iloc[0]
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Temperature", f"{row['temperature_2m']:.1f} C")
    with col2:
        st.metric("Humidity", f"{row['relativehumidity_2m']:.0f}%")
    with col3:
        st.metric("Precipitation", f"{row['precipitation']:.1f} mm")
    with col4:
        if city in freshness:
            last_ts = freshness[city]["latest_timestamp"]
            if isinstance(last_ts, pd.Timestamp):
                age_hours = (
                    datetime.now(timezone.utc).replace(tzinfo=None) - last_ts
                ).total_seconds() / 3600
                st.metric("Data Age", f"{age_hours:.0f}h ago")
            else:
                st.metric("Data Age", "N/A")
        else:
            st.metric("Data Age", "N/A")
