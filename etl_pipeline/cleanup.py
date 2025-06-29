from pathlib import Path

from .config import Config


def cleanup_weather_files(
    config: Config
) -> None:
    """
    Clean up the weather data files by removing the raw and
    processed files.

    Args:
        config (Config): Configuration object containing paths
        for raw and processed data.

    Returns:
        None: Deletes the raw and processed weather data files.
    """
    # extract paths from config
    raw_path:       Path = config.raw_path
    processed_path: Path = config.processed_path

    # remove raw file if it exists
    if raw_path.exists():
        raw_path.unlink()

    # remove processed file if it exists
    if processed_path.exists():
        processed_path.unlink()
