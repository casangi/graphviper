import warnings, time, os, psutil, multiprocessing, re
import dask
import copy
import os
import sys
import logging
import graphviper
import distributed
from graphviper.parameter_checking.check_logger_parms import (
    check_logger_parms,
    check_worker_logger_parms,
)
from graphviper.logger import setup_logger, get_logger
from graphviper.dask._worker import (
    _worker,
)  # _worker_logger_plugin


def local_client(
    cores: int = None,
    memory_limit: str = None,
    autorestrictor: bool = False,
    dask_local_dir: str = None,
    local_dir: str = None,
    wait_for_workers: bool = True,
    log_parms: dict = {},
    worker_log_parms: dict = {},
):
    """
    Creates a Dask local cluster and setups the logger.

    Parameters
    ----------
    cores : int, optional
        Number of cores in local Dask cluster.
    memory_limit : str, optional
        Amount of memory per core.
    autorestrictor : bool, optional
        Experimental modified Dask schedular.
    dask_local_dir : str, optional
         Where Dask should store temporary files, defaults to None. If None Dask will use "./dask-worker-space".
    local_dir : str, optional
        Used for testing local caching of data. 
    wait_for_workers : bool, optional
        _description_, by default True
    log_params : dict, optional
        The logger for the main process (code that does not run in parallel), defaults to log_params = 
            {
                "log_to_term":True,
                "log_level":"INFO",
                "log_to_file":False,
                "log_file":"viper_"
            }
    log_params["log_to_term"]: bool, optional
        Prints logging statements to the terminal, default to True.
    log_params["log_level"]: str, optional
        Log level options are: "CRITICAL", "ERROR", "WARNING", "INFO", and "DEBUG". \
        With defaults of "INFO".
    log_params["log_to_file"]: bool, optional
        Write log to file, defaults to False.
    log_params["log_file"]: str, optional
        If log_params["log_to_file"] is True the log will be written to a file with the name: \
        log_params["log_file"].
    worker_log_params: dict, optional
        worker_log_params: Keys as same as log_params, default to worker_log_params =
            {
                "log_to_term":False,
                "log_level":"INFO",
                "log_to_file":False,
                "log_file":None
            }
    Returns
    -------
    distributed.Client
        Dask Distributed Client
    """

    _log_parms = copy.deepcopy(log_parms)
    _worker_log_parms = copy.deepcopy(worker_log_parms)

    assert check_logger_parms(
        _log_parms
    ), "######### ERROR: initialize_processing log_parms checking failed."

    if _worker_log_parms is not None:
        assert check_worker_logger_parms(
            _worker_log_parms
        ), "######### ERROR: initialize_processing log_parms checking failed."

    if local_dir:
        os.environ["VIPER_LOCAL_DIR"] = local_dir
        local_cache = True
    else:
        local_cache = False

    # print(_log_parms)
    setup_logger(**_log_parms)
    logger = get_logger()

    _set_up_dask(dask_local_dir)

    viper_path = graphviper.__path__[0]
    if local_cache or autorestrictor:
        dask.config.set(
            {
                "distributed.scheduler.preload": os.path.join(
                    viper_path, "dask/_scheduler.py"
                )
            }
        )

        dask.config.set(
            {
                "distributed.scheduler.preload-argv": [
                    "--local_cache",
                    local_cache,
                    "--autorestrictor",
                    autorestrictor,
                ]
            }
        )

    # This method of assigning a worker plugin does not seem to work when using dask_jobqueue. Consequently using client.register_plugin so that the method of assigning a worker plugin is the same for local_client and slurm_cluster_client.
    # if local_cache or _worker_log_parms:
    #     dask.config.set({"distributed.worker.preload": os.path.join(viper_path,"_utils/_worker.py")})
    #     dask.config.set({"distributed.worker.preload-argv": ["--local_cache",local_cache,"--log_to_term",_worker_log_parms["log_to_term"],"--log_to_file",_worker_log_parms["log_to_file"],"--log_file",_worker_log_parms["log_file"],"--log_level",_worker_log_parms["log_level"]]})

    # setup distributed based multiprocessing environment
    if cores is None:
        cores = multiprocessing.cpu_count()
    if memory_limit is None:
        memory_limit = (
            str(round(((psutil.virtual_memory().available / (1024**2))) / cores))
            + "MB"
        )
    cluster = distributed.LocalCluster(
        n_workers=cores, threads_per_worker=1, processes=True, memory_limit=memory_limit
    )  # , silence_logs=logging.ERROR #,resources={"GPU": 2}
    client = distributed.Client(cluster)
    client.get_versions(check=True)

    # When constructing a graph that has local cache enabled all workers need to be up and running.

    if local_cache or wait_for_workers:
        client.wait_for_workers(n_workers=cores)

    if local_cache or _worker_log_parms:
        plugin = _worker(local_cache, _worker_log_parms)

        if sys.version_info[1] < 9:
            client.register_worker_plugin(plugin, name="viper_worker")
        else:
            client.register_plugin(plugin, name="viper_worker")
    logger.info("Created client " + str(client))

    return client


def slurm_cluster_client(
    workers_per_node: int,
    cores_per_node: int,
    memory_per_node: str,
    number_of_nodes: int,
    queue: str,
    interface: str,
    python_env_dir: str,
    dask_local_dir: str,
    dask_log_dir: str,
    exclude_nodes: str,
    dashboard_port: int,
    local_dir: str = None,
    autorestrictor: bool = False,
    wait_for_workers: bool = True,
    log_parms={},
    worker_log_parms={},
):
    """
    Creates a Dask slurm_cluster_client on a multinode cluster.

    Parameters
    ----------
    cores : int, optional
        Number of cores in local Dask cluster.
    memory_limit : str, optional
        Amount of memory per core.
    autorestrictor : bool, optional
        Experimental modified Dask schedular.
    dask_local_dir : str, optional
         Where Dask should store temporary files, defaults to None. If None Dask will use "./dask-worker-space".
    local_dir : str, optional
        Used for testing local caching of data. 
    wait_for_workers : bool, optional
        _description_, by default True
    log_params : dict, optional
        The logger for the main process (code that does not run in parallel), defaults to log_params = 
            {
                "log_to_term":True,
                "log_level":"INFO",
                "log_to_file":False,
                "log_file":"viper_"
            }
    log_params["log_to_term"]: bool, optional
        Prints logging statements to the terminal, default to True.
    log_params["log_level"]: str, optional
        Log level options are: "CRITICAL", "ERROR", "WARNING", "INFO", and "DEBUG". \
        With defaults of "INFO".
    log_params["log_to_file"]: bool, optional
        Write log to file, defaults to False.
    log_params["log_file"]: str, optional
        If log_params["log_to_file"] is True the log will be written to a file with the name: \
        log_params["log_file"].
    worker_log_params: dict, optional
        worker_log_params: Keys as same as log_params, default to worker_log_params =
            {
                "log_to_term":False,
                "log_level":"INFO",
                "log_to_file":False,
                "log_file":None
            }
    Returns
    -------
    distributed.Client
        Dask Distributed Client
    """

    # https://github.com/dask/dask/issues/5577

    from dask_jobqueue import SLURMCluster
    from distributed import Client, config, performance_report

    _log_parms = copy.deepcopy(log_parms)
    _worker_log_parms = copy.deepcopy(worker_log_parms)

    assert _check_logger_parms(
        _log_parms
    ), "######### ERROR: initialize_processing log_parms checking failed."
    assert _check_worker_logger_parms(
        _worker_log_parms
    ), "######### ERROR: initialize_processing log_parms checking failed."

    if local_dir:
        os.environ["VIPER_LOCAL_DIR"] = local_dir
        local_cache = True
    else:
        local_cache = False

    # Viper logger for code that is not part of the Dask graph. The worker logger is setup in the _viper_worker plugin.
    from viper._utils._logger import setup_logger

    setup_logger(**_log_parms)
    logger = get_logger()

    _set_up_dask(dask_local_dir)

    viper_path = graphviper.__path__.__dict__["_path"][0]
    if local_cache or autorestrictor:
        dask.config.set(
            {
                "distributed.scheduler.preload": os.path.join(
                    viper_path, "_concurrency/_dask/_scheduler.py"
                )
            }
        )
        dask.config.set(
            {
                "distributed.scheduler.preload-argv": [
                    "--local_cache",
                    local_cache,
                    "--autorestrictor",
                    autorestrictor,
                ]
            }
        )

    # This method of assigning a worker plugin does not seem to work when using dask_jobqueue. Consequently using client.register_plugin so that the method of assigning a worker plugin is the same for local_client and slurm_cluster_client.
    # if local_cache or _worker_log_parms:
    #     dask.config.set({"distributed.worker.preload": os.path.join(viper_path,"_utils/_worker.py")})
    #     dask.config.set({"distributed.worker.preload-argv": ["--local_cache",local_cache,"--log_to_term",_worker_log_parms["log_to_term"],"--log_to_file",_worker_log_parms["log_to_file"],"--log_file",_worker_log_parms["log_file"],"--log_level",_worker_log_parms["log_level"]]})

    cluster = SLURMCluster(
        processes=workers_per_node,
        cores=cores_per_node,
        interface=interface,
        memory=memory_per_node,
        walltime="24:00:00",
        queue=queue,
        name="viper",
        python=python_env_dir,  # "/mnt/condor/jsteeb/viper_py/bin/python", #"/.lustre/aoc/projects/ngvla/viper/viper_py_env/bin/python",
        local_directory=dask_local_dir,  # "/mnt/condor/jsteeb",
        log_directory=dask_log_dir,
        job_extra_directives=["--exclude=" + exclude_nodes],
        # job_extra_directives=["--exclude=nmpost087,nmpost089,nmpost088"],
        scheduler_options={"dashboard_address": ":" + str(dashboard_port)},
    )  # interface="ib0"

    client = Client(cluster)

    cluster.scale(workers_per_node * number_of_nodes)

    # When constructing a graph that has local cache enabled all workers need to be up and running.

    if local_cache or wait_for_workers:
        client.wait_for_workers(n_workers=workers_per_node * number_of_nodes)

    if local_cache or _worker_log_parms:
        plugin = _worker(local_cache, _worker_log_parms)
        if sys.version_info[1] < 9:
            client.register_worker_plugin(plugin, name="viper_worker")
        else:
            client.register_plugin(plugin, name="viper_worker")

    logger.info("Created client " + str(client))

    return client


def _set_up_dask(local_directory):
    if local_directory:
        dask.config.set({"temporary_directory": local_directory})
    dask.config.set({"distributed.scheduler.allowed-failures": 10})
    dask.config.set({"distributed.scheduler.work-stealing": True})
    dask.config.set({"distributed.scheduler.unknown-task-duration": "99m"})
    dask.config.set({"distributed.worker.memory.pause": False})
    dask.config.set({"distributed.worker.memory.terminate": False})
    # dask.config.set({"distributed.worker.memory.recent-to-old-time": "999s"})
    dask.config.set({"distributed.comm.timeouts.connect": "3600s"})
    dask.config.set({"distributed.comm.timeouts.tcp": "3600s"})
    dask.config.set({"distributed.nanny.environ.OMP_NUM_THREADS": 1})
    dask.config.set({"distributed.nanny.environ.MKL_NUM_THREADS": 1})
    # https://docs.dask.org/en/stable/how-to/customize-initialization.html
