import pytest
from etl_pipeline.config import Config, SecretsConfig
from etl_pipeline.load import save_weather_data
from pydantic import SecretStr


def test_processed_path_not_found(tmp_path):
    """
    Test that FileNotFoundError is raised when processed path doesn't exist.
    """
    config = Config(
        latitude=0.0,
        longitude=0.0,
        secrets=SecretsConfig(api_key=SecretStr("x" * 32)),
        raw_path=tmp_path / "raw.json",
        processed_path=tmp_path / "processed.csv",
        sink_path=tmp_path / "data.csv",
    )

    with pytest.raises(
        FileNotFoundError, match="Processed data file not found"
    ):
        save_weather_data(config=config)


def test_successful_save_new_file(tmp_path):
    """
    Test saving processed data when sink file doesn't exist yet.
    """
    # Create test data
    processed_data = "Test data for saving"

    # Setup paths
    processed_path = tmp_path / "processed.csv"
    sink_path = tmp_path / "data.csv"

    # Write test data
    processed_path.write_text(processed_data)

    # Create config
    config = Config(
        latitude=0.0,
        longitude=0.0,
        secrets=SecretsConfig(api_key=SecretStr("x" * 32)),
        raw_path=tmp_path / "raw.json",
        processed_path=processed_path,
        sink_path=sink_path,
    )

    # Run function
    save_weather_data(config=config)

    # Verify the sink file contains the processed data
    assert sink_path.read_text() == processed_data


def test_append_to_existing_file(tmp_path):
    """
    Test that data is appended, not overwritten.
    """
    existing_data = "Existing content\n"
    new_data = "New data to append"

    processed_path = tmp_path / "processed.csv"
    sink_path = tmp_path / "data.csv"

    processed_path.write_text(new_data)
    sink_path.write_text(existing_data)

    # Create config
    config = Config(
        latitude=0.0,
        longitude=0.0,
        secrets=SecretsConfig(api_key=SecretStr("x" * 32)),
        raw_path=tmp_path / "raw.json",
        processed_path=processed_path,
        sink_path=sink_path,
    )

    # Run function to append data
    save_weather_data(config=config)

    assert sink_path.read_text() == existing_data + new_data
