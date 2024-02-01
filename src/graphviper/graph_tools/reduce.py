import dask

from typing import Callable, Any
from typing import Dict, List


def reduce(
        graph: List,
        reduce_node_task: Callable[..., Any],
        input_params: Dict,
        mode: {"tree", "single_node"}="tree"
) -> List:
    """_summary_

    Parameters
    ----------
    graph : list
        Graph produced by :func:`graphviper.graph_tools.map`_.
    reduce_node_task : _type_
        _description_
    input_params : Dict
        _description_
    mode : tree&quot;,&quot;single_node, optional
        _description_, by default "tree"

    Returns
    -------
    list
        _description_
    """
    graph_reduced = None

    if mode == "tree":
        graph_reduced = _tree_combine(
            graph[0], reduce_node_task, input_params
        )
    elif mode == "single_node":
        graph_reduced = _single_node(
            graph[0], reduce_node_task, input_params
        )

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
