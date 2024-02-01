import psutil
import multiprocessing
import dask
import inspect
import importlib
import importlib.util
import os
import sys
import logging
import menrva
import pathlib
import distributed

import graphviper.utils.parameter as parameter
import graphviper.utils.logger as logger
import graphviper.utils.console as console

from distributed.diagnostics.plugin import WorkerPlugin

from graphviper.dask.plugins.worker import DaskWorker

from typing import Union, Dict, Any, Callable, Tuple

colorize = console.Colorize()


class MenrvaClient(distributed.Client):
    """
    This and extended version of the general Dask distributed client that will allow for
    plugin management and more extended features.
    """

    @staticmethod
    def call(func: Callable, *args: Tuple[Any], **kwargs: Dict[str, Any]):
        try:
            params = inspect.signature(func).bind(*args, **kwargs)
            return func(*params.args, **params.kwargs)

        except TypeError as e:
            logger.error("There was an error calling the function: {}".format(e))

    @staticmethod
    def instantiate_module(plugin: str, plugin_file: str, *args: Tuple[Any], **kwargs: Dict[str, Any]) -> WorkerPlugin:
        """

        Args:
            plugin (str): Name of plugin module.
            plugin_file (str): Name of module file. ** This should be moved into the module itself not passed **
            *args (tuple(Any)): This is any *arg that needs to be passed to the plugin module.
            **kwargs (dict[str, Any]): This is any **kwarg default values that need to be passed to the plugin module.

        Returns:
            Instance of plugin class.
        """
        spec = importlib.util.spec_from_file_location(plugin, plugin_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for member in inspect.getmembers(module, predicate=inspect.isclass):
            plugin_instance = getattr(module, member[0])
            logger.debug("Loading plugin module: {}".format(plugin_instance))
            return MenrvaClient.call(plugin_instance, *args, **kwargs)

    def load_plugin(
            self,
            directory: str,
            plugin: str,
            name: str,
            *args: Union[Tuple[Any], Any],
            **kwargs: Union[Dict[str, Any], Any]
    ):
        '''

        Parameters
        ----------
        directory :
        plugin :
        name :
        args :
        kwargs :

        Returns
        -------

        '''

        plugin_file = ".".join((plugin, "py"))
        if pathlib.Path(directory).joinpath(plugin_file).exists():
            plugin_instance = MenrvaClient.instantiate_module(
                plugin=plugin,
                plugin_file="/".join((directory, plugin_file)),
                *args, **kwargs
            )
            logger.debug(f"{plugin}")
            if sys.version_info.major == 3:
                if sys.version_info.minor > 8:
                    self.register_plugin(plugin_instance, name=name)

                else:
                    self.register_worker_plugin(plugin_instance, name=name)
            else:
                logger.warning("Python version may not be supported.")
        else:
            logger.error("Cannot find plugins directory: {}".format(colorize.red(directory)))


@parameter.validate(
    logger=logger.get_logger(logger_name="graphviper"),
    config_dir=str(pathlib.Path(__file__).parent.resolve().joinpath("config/"))
)
def local_client(
        cores: int = None,
        memory_limit: str = None,
        autorestrictor: bool = False,
        dask_local_dir: str = None,
        local_dir: str = None,
        wait_for_workers: bool = True,
        log_params: Union[None, Dict] = None,
        worker_log_params: Union[None, Dict] = None
) -> distributed.Client:
    """ Setup dask cluster and logger.

    :param cores: Number of cores in Dask cluster, defaults to None
    :type cores: int, optional

    :param memory_limit: Amount of memory per core. It is suggested to use '8GB', defaults to None
    :type memory_limit: str, optional

    :param autorestrictor: Boolean determining usage of autorestrictor plugin, defaults to False
    :type autorestrictor: False, optional

    :param local_dir: Defines client local directory, defaults to None
    :type local_dir: str, optional

    :param wait_for_workers: Boolean determining usage of wait_for_workers option in dask, defaults to False
    :type wait_for_workers: False, optional

    :param dask_local_dir: Where Dask should store temporary files, defaults to None. If None Dask will use \
    `./dask-worker-space`, defaults to None
    :type dask_local_dir: str, optional

    :param log_params: The logger for the main process (code that does not run in parallel), defaults to {}
    :type log_params: dict, optional

    :param log_params['log_to_term']: Prints logging statements to the terminal, default to True.
    :type log_params['log_to_term']: bool, optional

    :param log_params['log_level']: Log level options are: 'CRITICAL', 'ERROR', 'WARNING', 'INFO', and 'DEBUG'. \
    With defaults of 'INFO'.
    :type log_params['log_level']: bool, optional

    :param log_params['log_to_file']: Write log to file, defaults to False.
    :type log_params['log_to_file']: bool, optional

    :param log_params['log_file']: Log file name be written.
    :type log_params['log_file']: bool, optional

    :param worker_log_params: worker_log_params: Keys as same as log_params, default values given in `Additional \
    Information`_.
    :type worker_log_params: dict, optional

    :return: Dask Distributed Client
    :rtype: distributed.Client


    .. _Additional Information:

    **Additional Information**

    ``worker_log_params`` default values are set internally when there is not user input. The default values are given\
     below.

    .. parsed-literal::
        worker_log_params =
            {
                'log_to_term':False,
                'log_level':'INFO',
                'log_to_file':False,
                'log_file':None
            }

    **Example Usage**

    .. parsed-literal::
        from menrva.client import local_client

        client = local_client(
            cores=2,
            memory_limit='8GB',
            log_params={
                'log_level':'DEBUG'
            }
        )

    """
    colorize = console.Colorize()

    if log_params is None:
        log_params = {
            'logger_name': "client",
            'log_to_term': True,
            'log_level': 'INFO',
            'log_to_file': False,
            'log_file': None
        }

    if worker_log_params is None:
        worker_log_params = {
            'logger_name': "worker",
            'log_to_term': True,
            'log_level': 'INFO',
            'log_to_file': False,
            'log_file': None
        }

    if local_dir:
        os.environ['CLIENT_LOCAL_DIR'] = local_dir
        local_cache = True

    else:
        local_cache = False

    logger.setup_logger(**log_params)

    if dask_local_dir is None:
        logger.warning(f"It is recommended that the local cache directory be set using "
                       f"the {colorize.blue('dask_local_dir')} parameter.")

    _set_up_dask(dask_local_dir)

    # This will work as long as the scheduler path isn't in some outside directory. Being that it is a plugin specific
    # to this module, I think keeping it static in the module directory it good.
    plugin_path = str(pathlib.Path(__file__).parent.resolve().joinpath("plugins/"))

    if local_cache or autorestrictor:
        dask.config.set({
            "distributed.scheduler.preload": os.path.join(plugin_path, 'scheduler.py')
        })

        dask.config.set({
            "distributed.scheduler.preload-argv": [
                "--local_cache", local_cache,
                "--autorestrictor", autorestrictor
            ]
        })

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
        memory_limit = str(round((psutil.virtual_memory().available / (1024 ** 2)) / cores)) + 'MB'

    cluster = distributed.LocalCluster(
        n_workers=cores,
        threads_per_worker=1,
        processes=True,
        memory_limit=memory_limit,
        silence_logs=logging.ERROR  # , silence_logs=logging.ERROR #,resources={ 'GPU': 2}
    )

    client = MenrvaClient(cluster)
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
            log_params=worker_log_params
        )

    logger.info('Created client ' + str(client))

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
    """
    Creates a Dask slurm_cluster_client on a multinode cluster.

    interface eth0, ib0
    """

    # https://github.com/dask/dask/issues/5577

    from dask_jobqueue import SLURMCluster
    from distributed import Client

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

    # viper_path = graphviper.__path__.__dict__["_path"][0]
    viper_path = menrva.__path__.__dict__["_path"][0]

    if local_cache or autorestrictor:
        dask.config.set(
            {
                "distributed.scheduler.preload": os.path.join(
                    viper_path, "plugins/scheduler.py"
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
    #    dask.config.set({"distributed.worker.preload": os.path.join(viper_path,"_utils/_worker.py")})
    #    dask.config.set({
    #    "distributed.worker.preload-argv": [
    #    "--local_cache",local_cache,
    #    "--log_to_term",worker_log_params["log_to_term"],
    #    "--log_to_file",worker_log_params["log_to_file"],
    #    "--log_file",worker_log_params["log_file"],
    #    "--log_level",worker_log_params["log_level"]]
    #    })
    #

    cluster = SLURMCluster(
        processes=workers_per_node,
        cores=cores_per_node,
        interface=interface,
        memory=memory_per_node,
        walltime="24:00:00",
        queue=queue,
        name="viper",
        python=python_env_dir,
        # "/mnt/condor/jsteeb/viper_py/bin/python", #"/.lustre/aoc/projects/ngvla/viper/viper_py_env/bin/python",
        local_directory=dask_local_dir,  # "/mnt/condor/jsteeb",
        log_directory=dask_log_dir,
        job_extra_directives=["--exclude=" + exclude_nodes],
        # job_extra_directives=["--exclude=nmpost087,nmpost089,nmpost088"],
        scheduler_options={"dashboard_address": ":" + str(dashboard_port)},
    )  # interface="ib0"

    client = Client(cluster)

    cluster.scale(workers_per_node * number_of_nodes)

    """
    When constructing a graph that has local cache enabled all workers need to be up and running.
    """
    if local_cache or wait_for_workers:
        client.wait_for_workers(n_workers=workers_per_node * number_of_nodes)

    if local_cache or worker_log_params:
        plugin = DaskWorker(local_cache, worker_log_params)

        if sys.version_info.minor < 9:
            client.register_worker_plugin(plugin, name="worker_logger")
        else:
            client.register_plugin(plugin, name="worker_logger")

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
