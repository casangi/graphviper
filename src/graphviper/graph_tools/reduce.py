import dask
from typing import Callable, Any, Union
from typing import Dict, List

from graphviper.graph_tools.graph import GraphNode, ReduceNode, CallableNode


def reduce(
    input: GraphNode,
    reduce_operator: Union[Callable[..., Any],CallableNode],
    input_params: Dict,
    mode: {"tree", "single_node"} = "tree",
) -> ReduceNode:
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
    if not isinstance(reduce_operator, CallableNode):
        reduce_operator = CallableNode(reduce_operator)
    return ReduceNode(input, reduce_operator, input_params, mode)
