import pytest
import duckdb
import pandas as pd
from datetime import datetime, timezone

from etl.load import create_weather_table, upsert_weather_data, backfill_city


@pytest.fixture
def conn():
    """Create an in-memory DuckDB connection for testing."""
    c = duckdb.connect(":memory:")
    yield c
    c.close()


def _make_df(city="TestCity", rows=3, start_hour=0):
    base = datetime(2024, 1, 1, start_hour)
    return pd.DataFrame(
        {
            "timestamp": pd.date_range(base, periods=rows, freq="h"),
            "temperature_2m": [20.0 + i for i in range(rows)],
            "relativehumidity_2m": [50.0] * rows,
            "precipitation": [0.0] * rows,
            "city": city,
            "latitude": -26.2,
            "longitude": 28.0,
            "load_date": datetime.now(timezone.utc).date(),
        }
    )


class TestCreateTable:
    def test_creates_table(self, conn):
        create_weather_table(conn)
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name='weather_hourly'"
        ).fetchall()
        assert len(tables) == 1

    def test_idempotent(self, conn):
        create_weather_table(conn)
        create_weather_table(conn)
        count = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='weather_hourly'"
        ).fetchone()[0]
        assert count == 1


class TestUpsert:
    def test_inserts_data(self, conn):
        df = _make_df()
        upsert_weather_data(conn, df)
        result = conn.execute("SELECT COUNT(*) FROM weather_hourly").fetchone()[0]
        assert result == 3

    def test_deduplicates_on_repeat(self, conn):
        df = _make_df()
        upsert_weather_data(conn, df)
        upsert_weather_data(conn, df)
        result = conn.execute("SELECT COUNT(*) FROM weather_hourly").fetchone()[0]
        assert result == 3

    def test_inserts_different_cities(self, conn):
        df1 = _make_df(city="CityA")
        df2 = _make_df(city="CityB")
        upsert_weather_data(conn, df1)
        upsert_weather_data(conn, df2)
        result = conn.execute("SELECT COUNT(*) FROM weather_hourly").fetchone()[0]
        assert result == 6


class TestBackfillCity:
    def test_backfills_null_city(self, conn):
        create_weather_table(conn)
        conn.execute(
            """
            INSERT INTO weather_hourly
                (timestamp, temperature_2m, relativehumidity_2m, precipitation,
                 city, latitude, longitude, load_date)
            VALUES ('2024-01-01 00:00:00', 20.0, 50.0, 0.0,
                    NULL, -26.2, 28.0, '2024-01-01')
            """
        )
        locations = [{"name": "Johannesburg", "latitude": -26.2, "longitude": 28.0}]
        backfill_city(conn, locations)
        city = conn.execute(
            "SELECT city FROM weather_hourly WHERE latitude=-26.2"
        ).fetchone()[0]
        assert city == "Johannesburg"
