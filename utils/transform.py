import json
from datetime import datetime
from pathlib import Path

from .config import Config


def process_weather_data(config: Config) -> None:
    """
    Process the raw weather data and save it to a new file.

    Args:
        config (Config): Configuration object containing paths for raw
        and processed data.

    Returns:
        None: Writes the processed weather data line to a CSV file.
    """
    # extract paths from config
    raw_path: Path = config.raw_path
    processed_path: Path = config.processed_path

    # ensure file at raw_path exists
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw data file not found: {raw_path}")

    # ensure the processed_path directory exists
    processed_path.parent.mkdir(parents=True, exist_ok=True)

    # fetch raw data
    with open(raw_path) as f:
        data = json.load(f)

    # extract relevant fields
    loc = data["name"]
    dt = data["dt"]
    desc = data["weather"][0]["description"]
    temp = data["main"]["temp"]
    clouds = data["clouds"]["all"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    pressure = data["main"]["pressure"]

    # create csv string
    csv_string = (
        f"{loc};"
        f"{dt};"
        f"{desc};"
        f"{temp};"
        f"{clouds};"
        f"{humidity};"
        f"{wind_speed};"
        f"{pressure}\n"
    )

    # if file exists, rename it with a date and time suffix
    if processed_path.exists():
        today = datetime.now().strftime("%Y%m%d%H%M%S")
        processed_path.rename(
            processed_path.with_name(processed_path.stem + f"_{today}.bak")
        )

    # touch csv file and dump processed data
    processed_path.touch(exist_ok=False)
    with open(processed_path, "w") as f:
        f.write(csv_string)
