from typing import Callable, Any
from xradio.vis.read_processing_set import read_processing_set
from xradio.image import read_image
import numpy as np
import dask
import math
import os
import datetime
import itertools
from scipy.interpolate import interp1d


def map(
    input_data_store: str,
    input_data_type: {"processing_set", "image"},
    node_task_data_mapping: dict,
    node_task: Callable[..., Any],
    input_parms: dict,
    ps_sel_parms: dict = {},
    client = None,
    date_time: str = None
):
    """Builds a perfectly parallel graph where a node is created for each chunk defined in parallel_coords.

    Parameters
    ----------
    input_data_store : str
        The file path of the input data.
    input_data_type : {"processing_set", "image"}
        The type of the input data. Currently "processing_set" and "image" are supported.
    parallel_coords : dict
        The parallel coordinates determine the parallelism of the map graph.
        The keys in the parallel coordinates can by any combination of the dimension coordinates in the input data.
        The values are XRADIO measures with an adittional key called data_chunks that devides the values in data into chunks.
        Example of parallel_coords['frequency'] with 3 chunks:
            data: [100, 200, 300, 400, 500]
            data_chunks
                0: [100, 200]
                1: [300, 400]
                2: [500]
            dims: ('frequency',)
            attrs:
                frame: 'LSRK'
                type: spectral_coord
                units: ['Hz']
    node_task : Callable[..., Any]
        The function that forms the nodes in the graph.
    input_parms : dict
        The input parameters to be passed to node_task.
    ps_sel_parms : dict, optional
        , by default {}
    client : optional
        The Dask client, by default None.
    date_time : optional

    Returns
    -------
    graph:
        Dask graph along with coordinates.
    """

    # Load Metadata
    if input_data_type == "processing_set":
        input_data = read_processing_set(
            input_data_store, ps_sel_parms["intents"], ps_sel_parms["fields"]
        )
    elif input_data_type == "image":
        input_data = {"image": read_image(input_data_store)}

    n_tasks = len(node_task_data_mapping)
    print(n_tasks)
    (
        local_cache,
        viper_local_dir,
        date_time,
        tasks_to_node_map,
        nodes_ip_list,
    ) = _local_cache_configuration(n_tasks,client)
    print(local_cache)

    graph_list = []
    for task_id, node_task_parameters in node_task_data_mapping.items():
        print(task_id, node_task_parameters.keys())

        input_parms = node_task_parameters
        input_parms["date_time"] = date_time
        input_parms["viper_local_dir"] = viper_local_dir
        input_parms["input_data_store"] = input_data_store
        input_parms["task_id"] = task_id

        if local_cache:
            node_ip = nodes_ip_list[tasks_to_node_map[task_id]]
            # logger.debug(
            #     "Task with chunk id "
            #     + str(chunk_id)
            #     + " is assigned to ip "
            #     + str(node_ip)
            # )
            input_parms["node_ip"] = node_ip
            with dask.annotate(resources={node_ip: 1}):
                graph_list.append(dask.delayed(node_task)(dask.delayed(input_parms)))
        else:
            # print("input_parms",input_parms)
            graph_list.append(dask.delayed(node_task)(dask.delayed(input_parms)))
    return graph_list, input_parms["date_time"]


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
        assert worker_ip in list(
            wi["resources"].keys()
        ), "local_cache enabled but workers have not been annotated. Make sure that local_cache has been set to True during client setup."
        if worker_ip not in nodes:
            nodes.append(worker_ip)
    return nodes
