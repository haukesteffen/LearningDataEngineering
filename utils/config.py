from pathlib import Path

import yaml
from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class SecretsConfig(BaseSettings):
    """Configuration class for secrets.

    Attributes:
        api_key (SecretStr): API key for accessing the OpenWeatherMap API.
    """

    api_key: SecretStr = Field(
        default=SecretStr(""),
        min_length=32,
        max_length=32,
        description="API key for accessing the OpenWeatherMap API.",
    )
    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent.parent / ".env"
    )


class Config(BaseModel):
    """Main configuration class.

    Attributes:
        latitude (float): Latitude of the weather location
        longitude (float): Longitude of the weather location
        secrets (SecretsConfig): Configuration for secrets
        raw_path (Path): Path to the raw data file.
        processed_path (Path): Path to the processed data file.
        sink_path (Path): Path to the data sink.
    """

    latitude: float = Field(
        default=0.0,
        ge=-90,
        le=90,
        description="Latitude of the location to collect weather data for.",
    )
    longitude: float = Field(
        default=0.0,
        ge=-180,
        le=180,
        description="Longitude of the location to collect weather data for.",
    )
    secrets: SecretsConfig
    raw_path: Path = Field(
        default=Path("data/raw/raw.json"),
        description="Path to the raw data file.",
    )
    processed_path: Path = Field(
        default=Path("data/processed/processed.csv"),
        description="Path to the processed data file.",
    )
    sink_path: Path = Field(
        default=Path("data/data.csv"),
        description="Path to the data sink.",
    )

    @classmethod
    def from_file(cls, path: str | Path) -> "Config":
        """Load configuration from YAML file.

        Args:
            path (Union[str, Path]): Path to the YAML configuration file.

        Returns:
            Config: An instance of the Config class with loaded parameters.

        Raises:
            AssertionError: If the configuration file does not exist or
            does not contain a dictionary.
        """
        # Check if the file path exists
        if isinstance(path, str):
            path = Path(path)
        if not path.exists():
            raise FileNotFoundError(
                f"Configuration file at {path} does not exist."
            )

        # Load YAML configuration file
        with open(path) as f:
            config_data = yaml.safe_load(f)
        if not isinstance(config_data, dict):
            raise ValueError(
                f"Configuration file at {path} must contain a dictionary."
            )

        # Load secrets from environment variables
        config_data["secrets"] = SecretsConfig()

        # Instantiate Config class
        return cls(**config_data)
