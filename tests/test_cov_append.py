"""Fast, self-contained unit tests for ``graphviper.graph_tools.append``.

Synthetic hand-built graphs only — no downloads, no distributed cluster, no
real measurement sets. Graphs are computed with the default
(synchronous/threaded) scheduler.
"""

import dask
import pytest

from graphviper.graph_tools.append import append as viper_append
from graphviper.graph_tools.generate_dask_workflow import generate_dask_workflow
from graphviper.graph_tools.reduce import reduce as viper_reduce


# --------------------------------------------------------------------------- #
# Helpers (module-level so they are picklable / inspectable like real tasks)
# --------------------------------------------------------------------------- #
def _map_v_graph(values):
    """Minimal map layer: one node per value, each returning its value."""

    def map_fn(input_params):
        return input_params["v"]

    return {
        "map": {
            "node_task": map_fn,
            "input_params": [{"v": v} for v in values],
        }
    }


def _reduce_sum(batch, input_params):
    return sum(batch) + input_params.get("offset", 0)


def _scale(input_data, input_params):
    return input_data * input_params["factor"]


def _shift(input_data, input_params):
    return input_data + input_params["shift"]


# --------------------------------------------------------------------------- #
# append() graph construction
# --------------------------------------------------------------------------- #
def test_append_records_step():
    """append() adds an ``{"node_task", "input_params"}`` step under
    graph["append"] and returns the same graph dict."""
    graph = _map_v_graph([1, 2])
    viper_reduce(graph, _reduce_sum, {}, mode="tree")

    returned = viper_append(graph, _scale, {"factor": 10})
    assert returned is graph
    assert graph["append"] == [{"node_task": _scale, "input_params": {"factor": 10}}]


def test_append_requires_reduce():
    """A single node is appended AFTER a reduce; a map-only graph is refused."""
    graph = _map_v_graph([1, 2])
    with pytest.raises(ValueError, match="reduce"):
        viper_append(graph, _scale, {"factor": 10})


def test_append_chains_in_call_order():
    """Repeated append() calls accumulate steps in call order."""
    graph = _map_v_graph([1, 2])
    viper_reduce(graph, _reduce_sum, {}, mode="single_node")
    viper_append(graph, _shift, {"shift": 1})
    viper_append(graph, _scale, {"factor": 2})

    assert [step["node_task"] for step in graph["append"]] == [_shift, _scale]


# --------------------------------------------------------------------------- #
# Dask backend execution
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("mode", ["tree", "tree_n", "single_node"])
def test_generate_dask_workflow_append(mode):
    """The appended node consumes the reduced result for every reduce mode:
    sum(1..4) = 10, then scale x10 -> 100. The result is a single node."""
    graph = _map_v_graph([1, 2, 3, 4])
    viper_reduce(graph, _reduce_sum, {}, mode=mode, n_batch=3)
    viper_append(graph, _scale, {"factor": 10})

    node = generate_dask_workflow(graph)
    assert hasattr(node, "compute")  # single delayed node, not a list
    assert dask.compute(node)[0] == 100


def test_generate_dask_workflow_append_chained_order():
    """Chained appended nodes run in call order: (10 + 1) * 2 = 22, not
    10 * 2 + 1 = 21."""
    graph = _map_v_graph([1, 2, 3, 4])
    viper_reduce(graph, _reduce_sum, {}, mode="tree")
    viper_append(graph, _shift, {"shift": 1})
    viper_append(graph, _scale, {"factor": 2})

    node = generate_dask_workflow(graph)
    assert dask.compute(node)[0] == 22


def test_generate_dask_workflow_append_sees_reduce_input_params():
    """The reduce's own input_params still apply before the appended node:
    (sum(1..4) + offset 5) * 2 = 30. (single_node mode so the offset applies
    exactly once; in the tree modes each reduce NODE applies it.)"""
    graph = _map_v_graph([1, 2, 3, 4])
    viper_reduce(graph, _reduce_sum, {"offset": 5}, mode="single_node")
    viper_append(graph, _scale, {"factor": 2})

    node = generate_dask_workflow(graph)
    assert dask.compute(node)[0] == 30
