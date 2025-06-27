import sys
from pathlib import Path

import pytest
from pydantic import SecretStr

# Add the project root directory to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from utils.config import Config, SecretsConfig


def test_config_from_file_not_found():
    """
    Test case for Config.from_file when the configuration file does not exist.
    This should raise a FileNotFoundError.
    """
    with pytest.raises(FileNotFoundError):
        Config.from_file(path=Path("non_existent_config.yaml"))


def test_config_from_file_invalid_yaml():
    """
    Test case for Config.from_file when the configuration file does not
    contain a dictionary. This should raise a ValueError.
    """
    invalid_config_path = "tests/invalid_config.yaml"
    with pytest.raises(ValueError, match=("must contain a dictionary.")):
        Config.from_file(path=Path(invalid_config_path))


def test_config_from_file_valid(mocker):
    """
    Test case for Config.from_file with a valid configuration file.
    This should return a Config object with the expected parameters.
    """
    mock_secrets = mocker.patch("utils.config.SecretsConfig")
    mock_secrets.return_value = SecretsConfig(api_key=SecretStr("x" * 32))

    config = Config.from_file(path=Path("tests/valid_config.yaml"))

    assert isinstance(config, Config)
    assert config.latitude == 37.7749
    assert config.longitude == -122.4194
    assert isinstance(config.secrets, SecretsConfig)
    assert config.secrets.api_key.get_secret_value() == "x" * 32
    assert config.raw_path.absolute() == Path("./data/raw/raw.json").absolute()
    assert (
        config.processed_path.absolute()
        == Path("./data/processed/processed.csv").absolute()
    )
    assert config.sink_path.absolute() == Path("./data/data.csv").absolute()
