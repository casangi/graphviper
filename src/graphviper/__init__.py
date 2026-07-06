import os
import warnings

from importlib.metadata import version
from toolviper.utils.logger import setup_logger

__version__ = version("graphviper")


def _suppress_zarr_unstable_warning():
    """Suppress zarr v3's ``UnstableSpecificationWarning`` package-wide.

    Zarr v3 emits this warning for fixed-length unicode string dtypes (e.g. the
    ``antenna_name`` / ``spectral_window_name`` coordinates used throughout the
    XRADIO processing sets GraphVIPER operates on). The warning is expected and
    only actionable upstream, so it is silenced on import. Prefer the exact
    warning category; fall back to a message match if zarr relocates the class.
    """
    try:
        from zarr.errors import UnstableSpecificationWarning

        warnings.filterwarnings("ignore", category=UnstableSpecificationWarning)
    except Exception:
        warnings.filterwarnings(
            "ignore", message=r".*does not have a Zarr V3 specification.*"
        )


_suppress_zarr_unstable_warning()

# Setup default logger instance for module
if not os.getenv("LOGGER_NAME"):
    os.environ["LOGGER_NAME"] = "graphviper"
    setup_logger(
        logger_name="graphviper",
        log_to_term=True,
        log_to_file=False,
        log_file="graphviper-logfile",
        log_level="INFO",
    )
