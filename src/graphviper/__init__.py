import os

from importlib.metadata import version
from graphviper.utils.logger import setup_logger

__version__ = version("graphviper")

# Setup default logger instance for module
if not os.getenv("VIPER_LOGGER_NAME"):
    os.environ["VIPER_LOGGER_NAME"] = "graphviper"
    setup_logger(
        logger_name="graphviper",
        log_to_term=True,
        log_to_file=False,
        log_file="graphviper-logfile",
        log_level="INFO",
    )

# Setup environment variable to identify the configuration directory for the module
if os.path.exists(os.path.dirname(__file__) + "/config/"):
    if not os.getenv("PARAMETER_CONFIG_PATH"):
        os.environ["PARAMETER_CONFIG_PATH"] = os.path.dirname(__file__) + "/config/"

    else:
        if os.path.dirname(__file__) + "/config/" not in os.getenv("PARAMETER_CONFIG_PATH"):
            os.environ["PARAMETER_CONFIG_PATH"] = ":".join(
                (os.environ["PARAMETER_CONFIG_PATH"], os.path.dirname(__file__) + "/config/")
            )
