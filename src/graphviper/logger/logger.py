logger_name = "viper"
import sys
import logging
from datetime import datetime

formatter = logging.Formatter(
    "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
)
from dask.distributed import get_worker
from dask.distributed import WorkerPlugin
import dask


def get_logger(name=logger_name):
    """
    Will first try to get worker logger. If this fails graph construction logger is returned.
    """
    from dask.distributed import get_worker

    try:
        worker = get_worker()
    except:
        return logging.getLogger(name)

    try:
        logger = worker.plugins["viper_worker"].get_logger()
        return logger
    except:
        return logging.getLogger()


def setup_logger(
    log_to_term=False,
    log_to_file=True,
    log_file="viper_",
    log_level="INFO",
    name=logger_name,
):
    """To setup as many loggers as you want"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.getLevelName(log_level))

    if log_to_term:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    if log_to_file:
        log_file = log_file + datetime.today().strftime("%Y%m%d_%H%M%S") + ".log"
        handler = logging.FileHandler(log_file)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def _get_worker_logger_name(name=logger_name):
    worker_log_name = name + "_" + str(get_worker().id)
    return worker_log_name


def setup_worker_logger(log_to_term, log_to_file, log_file, log_level, worker_id):
    # parallel_logger_name = _get_worker_logger_name()
    parallel_logger_name = logger_name + "_" + worker_id

    logger = logging.getLogger(parallel_logger_name)
    logger.setLevel(logging.getLevelName(log_level))

    if log_to_term:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    if log_to_file:
        log_file = (
            log_file
            + "_"
            + str(worker_id)
            + "_"
            + datetime.today().strftime("%Y%m%d_%H%M%S")
            + ".log"
        )
        handler = logging.FileHandler(log_file)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
