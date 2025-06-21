import os
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel


class Config(BaseModel):
    """Main configuration class.

    Attributes:
        latitude (float): Latitude of the location to collect
            weather data for.
        longitude (float): Longitude of the location to collect
            weather data for.
        api_key (str): API key for accessing the OpenWeatherMap
            API.
        raw_path (Path): Path to the directory for storing
            raw data.
        processed_path (Path): Path to the directory for storing
            processed data.
        sink_path (Path): Path to the data sink.
    """
    latitude: float
    longitude: float
    api_key: str
    raw_path: Path
    processed_path: Path
    sink_path: Path


def _convert_paths(data: dict) -> dict:
    """Recursively convert all dictionary values containing 'path'
    to Path objects.

    Args:
        data (dict): Dictionary containing configuration data.

    Returns:
        dict: Dictionary with 'path' strings converted to Path
        objects.
    """
    for key, value in data.items():
        if isinstance(value, dict):
            data[key] = _convert_paths(value)
        elif isinstance(value, str) and 'path' in key.lower():
            data[key] = Path(value)
    return data


def load_config(path: str | Path) -> Config:
    """Load configuration from YAML file.

    Args:
        path (Union[str, Path]): Path to the YAML configuration
        file.

    Returns:
        Config: An instance of the Config class with loaded parameters.
    """
    with open(path) as f:
        config = yaml.safe_load(f)

    # Convert all 'path' strings to Path objects
    config = _convert_paths(config)

    # Load api_key from .env file
    load_dotenv()
    config['api_key'] = os.getenv("OPENWEATHER_API_KEY")

    # Instantiate Config class
    config = Config(**config)
    print(f"Config loaded from {path}.")
    return config
