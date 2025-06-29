import json
from pathlib import Path

import pytest
from etl_pipeline.config import Config, SecretsConfig
from etl_pipeline.extract import fetch_weather_data
from pydantic import SecretStr


def test_request_invalid_status_code(mocker):
    """
    Test case for fetch_weather_data when the API request returns an error
    status code. This should raise an Exception with the appropriate error
    message.
    """
    # Mock the SecretsConfig to return a valid API key
    mock_secrets = mocker.patch("utils.config.SecretsConfig")
    mock_secrets.return_value = SecretsConfig(api_key=SecretStr("x" * 32))

    # Create a mock Config object with valid parameters
    mock_config = Config(
        latitude=0.0,
        longitude=0.0,
        secrets=mock_secrets.return_value,
        raw_path=Path("data/raw/raw.json"),
        processed_path=Path("data/processed/processed.csv"),
        sink_path=Path("data/data.csv"),
    )

    # Mock the requests.get call to return a response with an error status code
    mocker.patch(
        "utils.extract.requests.get",
        return_value=mocker.Mock(status_code=404, text="Not Found"),
    )

    # Call the fetch_weather_data function and expect it to raise an Exception
    with pytest.raises(
        Exception, match="Error fetching data: 404 - Not Found"
    ):
        fetch_weather_data(config=mock_config)


def test_request_success(mocker):
    """
    Test case for fetch_weather_data when the API request is successful.
    This should write the weather data to a JSON file.
    """
    # Mock the SecretsConfig to return a valid API key
    mock_secrets = mocker.patch("utils.config.SecretsConfig")
    mock_secrets.return_value = SecretsConfig(api_key=SecretStr("x" * 32))

    # Create a mock Config object with valid parameters
    mock_config = Config(
        latitude=0.0,
        longitude=0.0,
        secrets=mock_secrets.return_value,
        raw_path=Path("data/raw/raw.json"),
        processed_path=Path("data/processed/processed.csv"),
        sink_path=Path("data/data.csv"),
    )

    # Mock the requests.get call to return a successful response
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"weather": "sunny"}
    mocker.patch(
        "utils.extract.requests.get",
        return_value=mock_response,
    )

    # Mock the Path.exists method to simulate that the file does not exist
    mock_path_exists = mocker.patch(
        "utils.extract.Path.exists", return_value=False
    )

    # Mock the Path.rename method to simulate renaming the file
    mock_path_rename = mocker.patch("utils.extract.Path.rename")

    # Mock the Path.mkdir method to simulate creating the directory
    mock_mkdir = mocker.patch("utils.extract.Path.mkdir")

    # Mock the open function to simulate writing to a file
    mock_open = mocker.mock_open()
    mocker.patch("builtins.open", mock_open)

    # Mock json.dump to simulate writing JSON data
    mock_json_dump = mocker.patch("utils.extract.json.dump")

    # Call the fetch_weather_data function
    fetch_weather_data(config=mock_config)

    # Assertions to check if the function behaved as expected
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_path_exists.assert_called_once()
    mock_path_rename.assert_not_called()
    mock_open.assert_called_once_with(mock_config.raw_path, "w")
    mock_json_dump.assert_called_once_with({"weather": "sunny"}, mock_open())


def test_request_invalid_json(mocker):
    """
    Test case for fetch_weather_data when the API response is not valid JSON.
    This should raise an Exception with the appropriate error message.
    """
    # Mock the SecretsConfig to return a valid API key
    mock_secrets = mocker.patch("utils.config.SecretsConfig")
    mock_secrets.return_value = SecretsConfig(api_key=SecretStr("x" * 32))

    # Create a mock Config object with valid parameters
    mock_config = Config(
        latitude=0.0,
        longitude=0.0,
        secrets=mock_secrets.return_value,
        raw_path=Path("data/raw/raw.json"),
        processed_path=Path("data/processed/processed.csv"),
        sink_path=Path("data/data.csv"),
    )

    # Mock the requests.get call to return a response with invalid JSON
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = json.JSONDecodeError(
        "Expecting value", "doc", 0
    )
    mocker.patch(
        "utils.extract.requests.get",
        return_value=mock_response,
    )

    # Call the fetch_weather_data function and expect it to raise an Exception
    with pytest.raises(Exception, match="Invalid JSON in response"):
        fetch_weather_data(config=mock_config)
