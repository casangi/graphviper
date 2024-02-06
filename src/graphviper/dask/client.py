import psutil
import multiprocessing
import dask
import os
import dask_jobqueue
import logging
import pathlib
import distributed
import graphviper.dask.menrva

import graphviper.utils.parameter as parameter
import graphviper.utils.logger as logger
import graphviper.utils.console as console

from typing import Union, Dict


@parameter.validate()
def local_client(
    cores: int = None,
    memory_limit: str = None,
    autorestrictor: bool = False,
    dask_local_dir: str = None,
    local_dir: str = None,
    wait_for_workers: bool = True,
    log_params: Union[None, Dict] = None,
    worker_log_params: Union[None, Dict] = None,
) -> distributed.Client:
    """ Setup dask cluster and logger.

    Parameters
    ----------
    cores : int
        Number of cores in Dask cluster, defaults to None
    memory_limit : str
        Amount of memory per core. It is suggested to use '8GB', defaults to None
    autorestrictor : bool
        Boolean determining usage of autorestrictor plugin, defaults to False
    dask_local_dir : str
        Where Dask should store temporary files, defaults to None. If None Dask will use \
        `./dask-worker-space`, defaults to None
    local_dir : str
        Defines client local directory, defaults to None
    wait_for_workers : bool
        Boolean determining usage of wait_for_workers option in dask, defaults to False
    log_params : dict
        The logger for the main process (code that does not run in parallel), defaults to {}
    worker_log_params : dict
        worker_log_params: Keys as same as log_params, default values given in `Additional \
        Information`_.

    Returns
    -------
        Dask Distributed Client
    """

    colorize = console.Colorize()

    if log_params is None:
        log_params = {
            "logger_name": "client",
            "log_to_term": True,
            "log_level": "INFO",
            "log_to_file": False,
            "log_file": None,
        }

    if worker_log_params is None:
        worker_log_params = {
            "logger_name": "worker",
            "log_to_term": True,
            "log_level": "INFO",
            "log_to_file": False,
            "log_file": None,
        }

    if local_dir:
        os.environ["CLIENT_LOCAL_DIR"] = local_dir
        local_cache = True

    else:
        local_cache = False

    logger.setup_logger(**log_params)

    if dask_local_dir is None:
        logger.warning(
            f"It is recommended that the local cache directory be set using "
            f"the {colorize.blue('dask_local_dir')} parameter."
        )

    _set_up_dask(dask_local_dir)

    # This will work as long as the scheduler path isn't in some outside directory. Being that it is a plugin specific
    # to this module, I think keeping it static in the module directory it good.
    plugin_path = str(pathlib.Path(__file__).parent.resolve().joinpath("plugins/"))

    if local_cache or autorestrictor:
        dask.config.set(
            {"distributed.scheduler.preload": os.path.join(plugin_path, "scheduler.py")}
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

    # This method of assigning a worker plugin does not seem to work when using dask_jobqueue. Consequently, using \
    # client.register_worker_plugin so that the method of assigning a worker plugin is the same for local_client\
    # and slurm_cluster_client.
    # if local_cache or worker_log_params:
    #    dask.config.set({"distributed.worker.preload": os.path.join(path,'plugins/worker.py')})
    #    dask.config.set({"distributed.worker.preload-argv": ["--local_cache",local_cache,"--log_to_term",\
    #    worker_log_params['log_to_term'],"--log_to_file",worker_log_params['log_to_file'],"--log_file",\
    #    worker_log_params['log_file'],"--log_level",worker_log_params['log_level']]})

    # setup dask.distributed based multiprocessing environment
    if cores is None:
        cores = multiprocessing.cpu_count()

    if memory_limit is None:
        memory_limit = (
            str(round((psutil.virtual_memory().available / (1024**2)) / cores)) + "MB"
        )

    cluster = distributed.LocalCluster(
        n_workers=cores,
        threads_per_worker=1,
        processes=True,
        memory_limit=memory_limit,
        silence_logs=logging.ERROR,  # , silence_logs=logging.ERROR #,resources={ 'GPU': 2}
    )

    client = graphviper.dask.menrva.MenrvaClient(cluster)
    client.get_versions(check=True)

    # When constructing a graph that has local cache enabled all workers need to be up and running.
    if local_cache or wait_for_workers:
        client.wait_for_workers(n_workers=cores)

    logger.debug(f"These are the worker log parameters:\n {worker_log_params}")
    if local_cache or worker_log_params:
        client.load_plugin(
            directory=plugin_path,
            plugin="worker",
            name="worker_logger",
            local_cache=local_cache,
            log_params=worker_log_params,
        )

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
    log_params: Union[None, Dict] = None,
    worker_log_params: Union[None, Dict] = None,
):
    """Creates a Dask slurm_cluster_client on a multinode cluster.

        interface eth0, ib0

    Parameters
    ----------
    workers_per_node : int
    cores_per_node : int
    memory_per_node : str
    number_of_nodes : int
    queue : str
    interface : str
    python_env_dir : str
    dask_local_dir : str
    dask_log_dir : str
    exclude_nodes : str
    dashboard_port : int
    local_dir : str
    autorestrictor : bool
    wait_for_workers : bool
    log_params : dict
    worker_log_params : dict

    Returns
    -------
        distributed.Client
    """

    # https://github.com/dask/dask/issues/5577

    # from distributed import Client

    if log_params is None:
        log_params = {}

    if worker_log_params is None:
        worker_log_params = {}

    if local_dir:
        os.environ["VIPER_LOCAL_DIR"] = local_dir
        local_cache = True
    else:
        local_cache = False

    # Viper logger for code that is not part of the Dask graph. The worker logger is setup in the _worker plugin.
    # from viper._utils._logger import setup_logger

    logger.setup_logger(**log_params)

    _set_up_dask(dask_local_dir)

    plugin_path = str(pathlib.Path(__file__).parent.resolve().joinpath("plugins/"))

    if local_cache or autorestrictor:
        dask.config.set(
            {
                "distributed.scheduler.preload": os.path.join(
                    plugin_path, "plugins/scheduler.py"
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

    # This method of assigning a worker plugin does not seem to work when using dask_jobqueue. Consequently, using
    # client.register_plugin so that the method of assigning a worker plugin is the same for local_client and
    # slurm_cluster_client.
    #
    # if local_cache or worker_log_params:
    #    dask.config.set({"distributed.worker.preload": os.path.join(plugin_path,"_utils/_worker.py")})
    #    dask.config.set({
    #    "distributed.worker.preload-argv": [
    #    "--local_cache",local_cache,
    #    "--log_to_term",worker_log_params["log_to_term"],
    #    "--log_to_file",worker_log_params["log_to_file"],
    #    "--log_file",worker_log_params["log_file"],
    #    "--log_level",worker_log_params["log_level"]]
    #    })
    #

    cluster = dask_jobqueue.SLURMCluster(
        processes=workers_per_node,
        cores=cores_per_node,
        interface=interface,
        memory=memory_per_node,
        walltime="24:00:00",
        queue=queue,
        name="viper",
        python=python_env_dir,
        local_directory=dask_local_dir,
        log_directory=dask_log_dir,
        job_extra_directives=["--exclude=" + exclude_nodes],
        # job_extra_directives=["--exclude=nmpost087,nmpost089,nmpost088"],
        scheduler_options={"dashboard_address": ":" + str(dashboard_port)},
    )  # interface="ib0"

    client = graphviper.dask.menrva.MenrvaClient(cluster)

    cluster.scale(workers_per_node * number_of_nodes)

    # When constructing a graph that has local cache enabled all workers need to be up and running.

    if local_cache or wait_for_workers:
        client.wait_for_workers(n_workers=workers_per_node * number_of_nodes)

    if local_cache or worker_log_params:
        client.load_plugin(
            directory=plugin_path,
            plugin="worker",
            name="worker_logger",
            local_cache=local_cache,
            log_params=worker_log_params,
        )

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
