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
    graph_reduced = None

    if mode == "tree":
        graph_reduced = _tree_combine(graph[0], reduce_node_task, input_params)
    elif mode == "single_node":
        graph_reduced = _single_node(graph[0], reduce_node_task, input_params)

    return [graph_reduced, graph[1]]


def _tree_combine(list_to_combine, reduce_node_task, input_params):
    while len(list_to_combine) > 1:
        new_list_to_combine = []
        for i in range(0, len(list_to_combine), 2):
            if i < len(list_to_combine) - 1:
                lazy = dask.delayed(reduce_node_task)(
                    [list_to_combine[i], list_to_combine[i + 1]],
                    input_params,
                )
            else:
                lazy = list_to_combine[i]
            new_list_to_combine.append(lazy)
        list_to_combine = new_list_to_combine
    return list_to_combine


def _single_node(graph, reduce_node_task, input_params):
    return dask.delayed(reduce_node_task)(graph, input_params)
