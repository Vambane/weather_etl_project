import pytest
import pandas as pd
from datetime import datetime, timedelta, timezone

from etl.transform import transform_weather_data


def _make_raw_json(hours=168, temp_range=(10, 30), humidity_range=(40, 80)):
    """Build a valid raw JSON fixture for testing."""
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(hours=hours - 1)
    timestamps = [
        (start + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M")
        for i in range(hours)
    ]
    return {
        "hourly": {
            "time": timestamps,
            "temperature_2m": [
                temp_range[0] + (temp_range[1] - temp_range[0]) * (i / hours)
                for i in range(hours)
            ],
            "relativehumidity_2m": [
                humidity_range[0]
                + (humidity_range[1] - humidity_range[0]) * (i / hours)
                for i in range(hours)
            ],
            "precipitation": [0.0] * hours,
        }
    }


class TestTransformStructureValidation:
    def test_missing_hourly_section(self):
        with pytest.raises(ValueError, match="Missing 'hourly'"):
            transform_weather_data({}, 0.0, 0.0, "Test")

    def test_missing_required_field(self):
        raw = {"hourly": {"time": [], "temperature_2m": [], "precipitation": []}}
        with pytest.raises(ValueError, match="Missing required field"):
            transform_weather_data(raw, 0.0, 0.0, "Test")


class TestTransformDQC:
    def test_valid_data_passes(self):
        raw = _make_raw_json()
        df = transform_weather_data(raw, -26.2, 28.0, "Johannesburg")
        assert len(df) == 168
        assert "city" in df.columns
        assert df["city"].iloc[0] == "Johannesburg"

    def test_dqc_disabled_skips_checks(self):
        raw = _make_raw_json(hours=100)
        df = transform_weather_data(
            raw, 0.0, 0.0, "Test", dqc_enabled=False
        )
        assert len(df) == 100

    def test_custom_expected_hours(self):
        raw = _make_raw_json(hours=48)
        df = transform_weather_data(
            raw, 0.0, 0.0, "Test", expected_hours=48
        )
        assert len(df) == 48

    def test_temperature_out_of_range_fails(self):
        raw = _make_raw_json()
        raw["hourly"]["temperature_2m"][0] = 100.0
        with pytest.raises(ValueError, match="Temperature"):
            transform_weather_data(raw, 0.0, 0.0, "Test")

    def test_humidity_out_of_range_fails(self):
        raw = _make_raw_json()
        raw["hourly"]["relativehumidity_2m"][0] = -5.0
        with pytest.raises(ValueError, match="Humidity"):
            transform_weather_data(raw, 0.0, 0.0, "Test")

    def test_negative_precipitation_fails(self):
        raw = _make_raw_json()
        raw["hourly"]["precipitation"][0] = -1.0
        with pytest.raises(ValueError, match="Negative precipitation"):
            transform_weather_data(raw, 0.0, 0.0, "Test")

    def test_duplicate_timestamps_fail(self):
        raw = _make_raw_json()
        raw["hourly"]["time"][1] = raw["hourly"]["time"][0]
        with pytest.raises(ValueError, match="Duplicate timestamps"):
            transform_weather_data(raw, 0.0, 0.0, "Test")

    def test_historical_skips_freshness(self):
        raw = _make_raw_json(hours=720)
        df = transform_weather_data(
            raw,
            0.0,
            0.0,
            "Test",
            dqc_enabled=True,
            expected_hours=168,
            is_historical=True,
        )
        assert len(df) == 720
