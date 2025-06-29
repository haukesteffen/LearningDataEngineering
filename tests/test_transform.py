import json

import pytest
from etl_pipeline.config import Config, SecretsConfig
from etl_pipeline.transform import process_weather_data
from pydantic import SecretStr


def test_raw_path_not_found(tmp_path):
    """
    Test that FileNotFoundError is raised when raw path doesn't exist.
    """
    config = Config(
        latitude=0.0,
        longitude=0.0,
        secrets=SecretsConfig(api_key=SecretStr("x" * 32)),
        raw_path=tmp_path / "raw.json",
        processed_path=tmp_path / "processed.csv",
        sink_path=tmp_path / "data.csv",
    )

    with pytest.raises(FileNotFoundError, match="Raw data file not found"):
        process_weather_data(config=config)


def test_successful_processing_new_file(tmp_path):
    """
    Test processing when output file doesn't exist yet.
    """
    # Create test data
    weather_data = {
        "name": "Test City",
        "dt": 1609459200,
        "weather": [{"description": "clear sky"}],
        "main": {"temp": 20.0, "humidity": 50, "pressure": 1012},
        "clouds": {"all": 1},
        "wind": {"speed": 5.0},
    }

    # Setup paths
    raw_path = tmp_path / "raw.json"
    processed_path = tmp_path / "processed" / "processed.csv"
    sink_path = tmp_path / "data.csv"

    # Write test data
    raw_path.write_text(json.dumps(weather_data))

    # Create config
    config = Config(
        latitude=0.0,
        longitude=0.0,
        secrets=SecretsConfig(api_key=SecretStr("x" * 32)),
        raw_path=raw_path,
        processed_path=processed_path,
        sink_path=sink_path,
    )

    # Run function
    process_weather_data(config=config)

    # Verify results
    assert processed_path.exists()
    expected_csv = "Test City;1609459200;clear sky;20.0;1;50;5.0;1012\n"
    assert processed_path.read_text() == expected_csv
