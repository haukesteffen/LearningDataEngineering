from pathlib import Path

from airflow.decorators import dag, task
from etl_pipeline.cleanup import cleanup_weather_files
from etl_pipeline.config import Config
from etl_pipeline.extract import fetch_weather_data
from etl_pipeline.load import save_weather_data
from etl_pipeline.transform import process_weather_data

# fetch parameters from configuration file
CONFIG_PATH = Path("./config.yaml")
config = Config.from_file(path=CONFIG_PATH)


@dag(dag_id="weather_etl", schedule="*/5 * * * *", catchup=False)
def process_weather():
    """
    DAG to perform ETL operations for weather data.
    """

    @task()
    def extract(config: Config) -> None:
        """
        Extracts weather data from OpenWeatherMap API and saves it to
        a raw file.

        Args:
            config (Config): Configuration object containing API key
            and coordinates.

        Returns:
            None: Writes the weather data to a JSON file.
        """
        fetch_weather_data(config=config)

    @task()
    def transform(config: Config) -> None:
        """
        Processes the raw weather data and saves it to a new file.

        Args:
            config (Config): Configuration object containing paths
            for raw and processed data.

        Returns:
            None: Writes the processed weather data line to a CSV file.
        """
        process_weather_data(config=config)

    @task()
    def load(config: Config) -> None:
        """
        Saves the processed weather data to a specified sink path.

        Args:
            config (Config): Configuration object containing paths
            for processed data and sink.

        Returns:
            None: Appends the processed data to the sink file.
        """
        save_weather_data(config=config)

    @task()
    def cleanup(config: Config) -> None:
        """
        Cleans up the weather data files by removing the raw and
        processed files.

        Args:
            config (Config): Configuration object containing paths
            for raw and processed data.

        Returns:
            None: Deletes the raw and processed weather data files.
        """
        cleanup_weather_files(config=config)

    (
        extract(config=config)
        >> transform(config=config)
        >> load(config=config)
        >> cleanup(config=config)
    )


dag = process_weather()
