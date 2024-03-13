import dask
from typing import Callable, Any
from typing import Dict, List


def reduce(
    graph: list,
    reduce_node_task: Callable[..., Any],
    input_params: Dict,
    mode: {"tree", "single_node"} = "tree",
) -> list:
    """Appends a reduce step to the graph created by the :func:`graphviper.graph_tools.map`. function.

    Parameters
    ----------
    graph : list
        Graph produced by :func:`graphviper.graph_tools.map`.
    reduce_node_task : _type_
       The function that forms the nodes in the reduce portion of the graph must have two parameters: ``input_data`` and ``input_params``. The ``input_data`` represents the output from the mapping nodes, while ``input_params`` comes from the ``reduce`` parameter with the same name.
    input_params : Dict
        The input parameters to be passed to ``node_task``.
    mode : {"tree","single_node"}, optional
        - ``single_node``: The output from all `map` nodes is sent to a single node,
        - ``tree``: The outputs are combined using a binary tree reduction, by default "tree".

    Returns
    -------
    list
        List of a single `dask.delayed <https://docs.dask.org/en/latest/delayed-api.html>`_ objects that represent the ``reduce`` Dask graph.
    """
    if mode == "tree":
        graph['reduce'] = {'mode':'tree','node_task':reduce_node_task,'input_params':input_params}
    elif mode == "single_node":
        graph['reduce'] = {'mode':'single_node','node_task':reduce_node_task,'input_params':input_params}

    return graph
