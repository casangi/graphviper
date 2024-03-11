import os
import math
import dask
import datetime

import numpy as np
import graphviper.utils.logger as logger

from typing import Dict, Union
from typing import Callable, Any, Tuple, List
from xradio.vis._processing_set import processing_set
import copy


def map(
    input_data: Union[Dict, processing_set],
    node_task_data_mapping: dict,
    node_task: Callable[..., Any],
    input_params: dict,
    in_memory_compute: bool = False,
    client=None,
    date_time: str = None,
) -> list:
    """Create a perfectly parallel graph where a node is generated for each item in the :ref:`node_task_data_mapping <node task data mapping>` using the function specified in the ``node_task`` parameter.

    Parameters
    ----------
    input_data : Union[Dict, processing_set]
        Can either be a `processing_set <https://github.com/casangi/xradio/blob/main/src/xradio/vis/_processing_set.py>`_ or a Dictionary of `xarray.Datasets <https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html>`_. Only coordinates are needed so no actual data is loaded into memory (except if ``in_memory_compute`` is True).
    node_task_data_mapping :
        Node task data mapping dictionary. See :ref:`notes <node task data mapping>` for structure of dictionary.
    node_task : Callable[..., Any]
        The function that forms the nodes in the graph. The function must have a single input parameter that must be a dictionary. The ``input_params``, along with graph and coordinate-related parameters, will be passed to this function.
    input_params : Dict
        The input parameters to be passed to ``node_task``.
        See notes for input parameters requirements.
    in_memory_compute : optional
        Setting ``in_memory_compute`` can lead to memory issues since all data is loaded into memory. Consequently, this option should only be used for testing purposes. If true the lazy arrays in ``input_data`` are loaded into memory using `xarray.Dataset.load <https://docs.xarray.dev/en/stable/generated/xarray.Dataset.load.html>`_ , by default False.
        See notes of how to access data in ``node_task``.
    client : optional
        The Dask client is only required if local caching is enabled see :func:`graphviper.dask.client.slurm_cluster_client` , by default None.
    date_time : str, optional
        Used to annotate local cache, by default None.

    Returns
    -------
    List:
        List of `dask.delayed <https://docs.dask.org/en/latest/delayed-api.html>`_ objects that represent the ``Dask`` graph.

    Notes
    -----
    The ``input_params`` dictionary will be passed to each instance of the ``node_task`` along with the following items from the :ref:`node_task_data_mapping <node task data mapping>`:

        - chunk_indices
        - parallel_dims
        - data_selection
        - task_coords
        - task_id

    If ``in_memory_compute`` is ``False`` then ``input_params["input_data_store"]`` must be provided with a ``MutableMapping`` where a ``Zarr Group`` has been stored or a path to a directory in file system where a ``Zarr DirectoryStore`` has been stored.

    If local caching is enabled the following will also be included with the `input_params` dictionary:

        - date_time
        - viper_local_dir

    Example of how to access data in ``node_task``::

        if input_params["input_data"] is None: #in_memory_compute==False
            ps = load_data(
                input_params["input_data_store"], data_selection=input_params["data_selection"]
            )
        else: #in_memory_compute==True
            ps = input_params["input_data"]

    , where ``load_data`` is appropriate function for your data.

    """
    n_tasks = len(node_task_data_mapping)

    # Get local_cache configuration if enabled in graphviper.dask.client.slurm_cluster_client.
    # local_cache will be True if enabled.
    (
        local_cache,
        viper_local_dir,
        date_time,
        tasks_to_node_map,
        nodes_ip_list,
    ) = _local_cache_configuration(n_tasks, client, date_time)

    input_param_list = []
    # Create a node in Dask graph for each task_id in node_task_data_mapping
    for task_id, node_task_parameters in node_task_data_mapping.items():
        logger.debug(str(task_id) + str(node_task_parameters.keys()))

        input_params.update(node_task_parameters)
        input_params["task_id"] = task_id

        if in_memory_compute:  # Data gets loaded into memory.
            input_params["input_data"] = _select_data(
                input_data, input_params["data_selection"]
            )
        else:
            input_params["input_data"] = None

        if local_cache:
            input_params["date_time"] = date_time
            input_params["viper_local_dir"] = viper_local_dir
            node_ip = nodes_ip_list[tasks_to_node_map[task_id]]
            input_params["node_ip"] = node_ip
            #with dask.annotate(resources={node_ip: 1}):
            #    graph_list.append(dask.delayed(node_task)(dask.delayed(input_params)))    
        else:
            input_params["date_time"] = None
        input_param_list.append(copy.deepcopy(input_params))

    graph = {'map':{'node_task':node_task,'input_params':input_param_list}}

    return graph

def _select_data(input_data, data_selection):
    if isinstance(input_data, processing_set):
        input_data_sel = processing_set()
    else:
        input_data_sel = {}

    for xds_name, xds_isel in data_selection.items():
        input_data_sel[xds_name] = input_data[xds_name].isel(xds_isel).load()

    return input_data_sel


def _local_cache_configuration(n_tasks, client, date_time):
    if "VIPER_LOCAL_DIR" in os.environ:
        local_cache = True
        viper_local_dir = os.environ["VIPER_LOCAL_DIR"]

        if date_time is None:
            date_time = datetime.datetime.utcnow().strftime("%y%m%d%H%M%S")
    else:
        local_cache = False
        viper_local_dir = None
        date_time = None
        tasks_to_node_map = None
        nodes_ip_list = None
        return local_cache, viper_local_dir, date_time, tasks_to_node_map, nodes_ip_list

    workers_info = client.scheduler_info()["workers"]
    nodes_ip_list = _get_unique_resource_ip(workers_info)
    n_nodes = len(nodes_ip_list)

    tasks_per_compute_node = math.floor(n_tasks / n_nodes + 0.5)
    if tasks_per_compute_node == 0:
        tasks_per_compute_node = 1
    tasks_to_node_map = np.repeat(np.arange(n_nodes), tasks_per_compute_node)

    if len(tasks_to_node_map) < n_tasks:
        n_pad = n_tasks - len(tasks_to_node_map)
        tasks_to_node_map = np.concatenate(
            [tasks_to_node_map, np.array([tasks_to_node_map[-1]] * n_pad)]
        )

    return local_cache, viper_local_dir, date_time, tasks_to_node_map, nodes_ip_list


def _get_unique_resource_ip(workers_info):
    nodes = []
    for worker, wi in workers_info.items():
        worker_ip = worker[worker.rfind("/") + 1 : worker.rfind(":")]
        assert worker_ip in list(wi["resources"].keys()), (
            "local_cache enabled but workers have not been annotated. Make sure that local_cache has been set to True "
            "during client setup."
        )
        if worker_ip not in nodes:
            nodes.append(worker_ip)
    return nodes
