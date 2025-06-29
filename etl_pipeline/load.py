from pathlib import Path

from .config import Config


def save_weather_data(
    config: Config
) -> None:
    """
    Save the processed weather data to a specified sink path.

    Args:
        config (Config): Configuration object containing paths for processed
        data and sink.

    Returns:
        None: Appends the processed data to the sink file.
    """
    # extract paths from config
    processed_path: Path = config.processed_path
    sink_path:      Path = config.sink_path

    # ensure processed data file exists
    if not processed_path.exists():
        raise FileNotFoundError(
            f"Processed data file not found: {processed_path}"
            )

    # read processed data
    with open(processed_path) as f:
        data_string = f.read()

    # ensure the sink_path directory exists
    sink_path.parent.mkdir(parents=True, exist_ok=True)

    # ensure the sink file exists, if not create it
    if not sink_path.exists():
        sink_path.touch(exist_ok=False)

    # append the data to the sink file
    with open(sink_path, 'a') as f:
        f.write(data_string)
