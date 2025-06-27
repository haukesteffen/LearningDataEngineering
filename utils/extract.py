import json
from datetime import datetime
from pathlib import Path

import requests
from pydantic import SecretStr

from .config import Config


def fetch_weather_data(config: Config) -> None:
    """
    Fetches current weather data from OpenWeatherMap API for given latitude
    and longitude.

    Args:
        config (Config): Configuration object containing API key and
        coordinates.

    Raises:
        Exception: If the API request fails or returns an error.

    Returns:
        None: Writes the weather data to a JSON file.
    """
    # extract parameters from config
    latitude: float = config.latitude
    longitude: float = config.longitude
    api_key: SecretStr = config.secrets.api_key
    raw_path: Path = config.raw_path

    # ensure the raw_path directory exists
    raw_path.parent.mkdir(parents=True, exist_ok=True)

    # fetch raw data from OpenWeatherMap API
    url = (
        f"https://api.openweathermap.org/data/2.5/weather?"
        f"lat={latitude}&lon={longitude}"
        f"&appid={api_key.get_secret_value()}"
        f"&units=metric"
    )
    res = requests.get(url)
    if res.status_code != 200:
        raise Exception(f"Error fetching data: {res.status_code} - {res.text}")

    # if the response is not JSON, raise an error
    try:
        response = res.json()
    except json.JSONDecodeError as e:
        raise ValueError("Invalid JSON in response") from e

    # if file exists, rename it with a date and time suffix
    if raw_path.exists():
        dt = datetime.now().strftime("%Y%m%d%H%M%S")
        raw_path.rename(raw_path.with_name(raw_path.stem + f"_{dt}.bak"))

    # write the response to the raw_path file
    with open(raw_path, "w") as f:
        json.dump(response, f)
