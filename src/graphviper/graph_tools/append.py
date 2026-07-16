from typing import Any, Callable, Dict


def append(
    graph: Dict,
    append_node_task: Callable[..., Any],
    input_params: Dict,
) -> Dict:
    """Appends a single node after the reduce step of a map/reduce graph.

    The appended node consumes the reduced result — use it for a
    post-processing step that must run once after the reduce completes (for
    example normalizing the combined result, writing a summary to disk, or
    reshaping the reduce output for the caller).

    Parameters
    ----------
    graph : Dict
        Graph produced by :func:`graphviper.graph_tools.map` and
        :func:`graphviper.graph_tools.reduce`. A reduce step must already be
        present (the appended node's input is the reduced result).
    append_node_task : Callable[..., Any]
        The function that forms the appended node. Like a reduce node task it
        must have two parameters: ``input_data`` and ``input_params``. The
        ``input_data`` is the output of the reduce step (or of the previously
        appended node, see Notes), while ``input_params`` comes from the
        ``append`` parameter with the same name.
    input_params : Dict
        The input parameters to be passed to ``append_node_task``.

    Returns
    -------
    Dict
        The input ``graph`` dict with the step added to its ``"append"`` entry
        (a list of ``{"node_task", "input_params"}`` dicts, one per appended
        node, in call order). No ``dask.delayed`` objects are constructed
        here; the actual node is built later by
        :func:`graphviper.graph_tools.generate_dask_workflow` (or executed by
        :func:`graphviper.graph_tools.processes_with_mpi`).

    Raises
    ------
    ValueError
        If the graph has no ``"reduce"`` entry: call
        :func:`graphviper.graph_tools.reduce` before ``append``.

    Notes
    -----
    ``append`` may be called several times on the same graph; each call chains
    one more single node, consuming the previous appended node's output:
    ``map -> reduce -> append_1 -> append_2 -> ...``.

    Both execution backends honour the appended nodes:
    :func:`graphviper.graph_tools.generate_dask_workflow` builds one
    ``dask.delayed`` node per step downstream of the reduce tree, and
    :func:`graphviper.graph_tools.processes_with_mpi` applies each step to the
    reduced result (on the manager by default, or on the worker pool when
    ``reduce_in_pool`` is set). The deprecated Airflow generator does not
    support ``append``.
    """
    if "reduce" not in graph:
        raise ValueError(
            "append requires a reduce step: call graphviper.graph_tools.reduce "
            "on the graph before append (the appended node consumes the "
            "reduced result)."
        )

    graph.setdefault("append", []).append(
        {"node_task": append_node_task, "input_params": input_params}
    )

    return graph
