import dask
from typing import Callable, Any
from typing import Dict

# Accepted reduction modes.  ``tree`` (binary) and ``single_node`` are the
# historical modes; ``tree_n`` is the variable-arity generalisation that combines
# ``n_batch`` nodes at each layer until a single node remains (``n_batch=2`` is
# equivalent to ``tree``; ``n_batch>=n`` collapses to a single layer like
# ``single_node``).
REDUCE_MODES = ("tree", "single_node", "tree_n")


def reduce(
    graph: Dict,
    reduce_node_task: Callable[..., Any],
    input_params: Dict,
    mode: {"tree", "single_node", "tree_n"} = "tree",
    n_batch: int = 2,
) -> Dict:
    """Appends a reduce step to the graph created by the :func:`graphviper.graph_tools.map`. function.

    Parameters
    ----------
    graph : list
        Graph produced by :func:`graphviper.graph_tools.map`.
    reduce_node_task : _type_
       The function that forms the nodes in the reduce portion of the graph must have two parameters: ``input_data`` and ``input_params``. The ``input_data`` represents the output from the mapping nodes, while ``input_params`` comes from the ``reduce`` parameter with the same name.
    input_params : Dict
        The input parameters to be passed to ``node_task``.
    mode : {"tree","single_node","tree_n"}, optional
        - ``single_node``: The output from all `map` nodes is sent to a single node,
        - ``tree``: The outputs are combined using a binary tree reduction (each
          reduce node combines two inputs), by default "tree".
        - ``tree_n``: The outputs are combined using a tree reduction in which each
          reduce node combines ``n_batch`` inputs per layer, repeating layer by
          layer until a single node remains.  ``n_batch=2`` reproduces ``tree``;
          a large ``n_batch`` reduces the number of layers (shallower tree, more
          inputs combined per node) trending toward ``single_node`` behaviour.
    n_batch : int, optional
        Number of inputs combined by each reduce node, per layer, when
        ``mode="tree_n"``.  Must be ``>= 2`` (validated unconditionally so a
        meaningless value can never be recorded on the graph).  Used only by
        ``mode="tree_n"``; the other modes ignore it.  Default 2.

    Returns
    -------
    list
        List of a single `dask.delayed <https://docs.dask.org/en/latest/delayed-api.html>`_ objects that represent the ``reduce`` Dask graph.

    Notes
    -----
    The chosen ``mode``/``n_batch`` are recorded on the graph and honoured by both
    execution backends: :func:`graphviper.graph_tools.generate_dask_workflow`
    (builds the corresponding ``dask.delayed`` reduce tree) and
    :func:`graphviper.graph_tools.processes_with_mpi` (applies the same fan-in when
    reducing the gathered map results).
    """
    if mode not in REDUCE_MODES:
        raise ValueError(
            f"Unknown reduce mode {mode!r}; expected one of {REDUCE_MODES}."
        )
    # Validate n_batch unconditionally (not just for tree_n) so a bad value can
    # never be silently stored on the graph. bool is an int subclass but
    # True/False < 2 is False/True respectively, so True is rejected as well.
    if not isinstance(n_batch, int) or isinstance(n_batch, bool) or n_batch < 2:
        raise ValueError(f"reduce n_batch must be an integer >= 2, got {n_batch!r}.")

    graph["reduce"] = {
        "mode": mode,
        "node_task": reduce_node_task,
        "input_params": input_params,
        "n_batch": n_batch,
    }

    return graph
