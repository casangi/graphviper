"""Coverage-focused unit tests for
``graphviper.graph_tools.generate_dask_workflow``.

These are synthetic and self-contained: no downloads, no distributed cluster, no
real measurement sets.  Graphs are either built by hand or with plain
``dask.delayed`` and computed with the default (synchronous/threaded) scheduler.

Targets uncovered lines:
    * 36     -> ``_tree_combine_n`` clamps ``n_batch < 2`` to 2 (binary tree).
    * 87     -> ``_prepare_task_input`` skips an ``xds_name`` absent from
               ``loaded_data``.
    * 92->94 -> ``_prepare_task_input`` skips ``.isel`` when the effective
               selection is empty (all ``slice(None)``).
    * 152    -> ``generate_dask_workflow`` per-task map fallback when
               ``load_node_id == -1`` inside a load graph.
    * 191    -> ``single_node`` reduce path (both branch directions).
"""

import dask
import numpy as np
import xarray as xr

from graphviper.graph_tools.generate_dask_workflow import (
    _prepare_task_input,
    _tree_combine_n,
    generate_dask_workflow,
)
from graphviper.graph_tools.reduce import reduce as viper_reduce


def _reduce_sum(batch, params):
    return sum(batch)


def test_tree_combine_n_clamps_nbatch_below_two():
    """Line 36: ``n_batch < 2`` is clamped to 2, so the reduction still runs as a
    binary tree and produces the correct total."""
    values = [1, 2, 3, 4, 5]

    node = _tree_combine_n(values, _reduce_sum, None, n_batch=1)
    # Result is a single dask.delayed node, not a plain int.
    assert hasattr(node, "compute")

    result = dask.compute(node)[0]
    assert result == sum(values) == 15


def test_prepare_task_input_missing_name_and_empty_selection():
    """Line 87 (missing xds_name -> continue) and branch 92->94 (empty effective
    selection -> no ``.isel``).  Also exercises 92->93 (non-empty -> ``.isel``)
    and the shallow-copy semantics."""
    ds_full = xr.Dataset({"vis": (["frequency"], np.arange(4.0))})
    ds_other = xr.Dataset({"vis": (["frequency"], np.arange(10.0, 14.0))})
    loaded_data = {"xds_full": ds_full, "xds_other": ds_other}

    relative_data_selection = {
        # All slice(None) -> effective_sel empty -> skip .isel (branch 92->94).
        "xds_full": {"frequency": slice(None)},
        # A real slice -> effective_sel non-empty -> .isel runs (branch 92->93).
        "xds_other": {"frequency": slice(0, 2)},
        # Absent from loaded_data -> continue (line 87).
        "xds_missing": {"frequency": slice(0, 2)},
    }

    input_params = {"task_id": 7, "flag": "orig"}
    result = _prepare_task_input(loaded_data, relative_data_selection, input_params)

    input_data = result["input_data"]

    # Missing name was skipped entirely.
    assert "xds_missing" not in input_data

    # slice(None) -> not sub-selected; object passed through unchanged.
    assert "xds_full" in input_data
    assert input_data["xds_full"] is ds_full
    assert input_data["xds_full"]["vis"].values.tolist() == [0.0, 1.0, 2.0, 3.0]

    # Real slice -> sub-selected to first two elements.
    assert input_data["xds_other"]["vis"].values.tolist() == [10.0, 11.0]

    # result is a shallow copy of input_params with input_data injected; the
    # caller's dict is untouched.
    assert result is not input_params
    assert result["task_id"] == 7
    assert result["flag"] == "orig"
    assert "input_data" not in input_params


def _make_full_xds():
    return xr.Dataset({"vis": (["frequency"], np.arange(4.0))})


def test_generate_dask_workflow_load_graph_with_minus_one_task():
    """Line 152: within a load graph, a task whose ``load_node_id == -1`` falls
    back to a per-task map node (no shared load node / prepare step), while a
    sibling task with a valid load node goes through ``_prepare_task_input``."""
    full = _make_full_xds()

    def load_fn(load_params):
        # Shared load node returns the whole (synthetic) disk chunk.
        assert load_params["marker"] == "load0"
        return {"xds_0": full}

    def map_fn(input_params):
        data = input_params["input_data"]
        if data is None:
            # Fallback (load_node_id == -1) path.
            return "fallback"
        return float(data["xds_0"]["vis"].values.sum())

    viper_graph = {
        "load": {
            "node_task": load_fn,
            "input_params": [{"marker": "load0"}],
        },
        "map": {
            "node_task": map_fn,
            "input_params": [
                {"input_data": None, "tag": "boundary"},  # load_node_id == -1
                {"tag": "in_chunk"},  # load_node_id == 0
            ],
            "load_node_ids": [-1, 0],
            "relative_data_selections": [
                None,
                {"xds_0": {"frequency": slice(0, 2)}},
            ],
        },
    }

    dask_graph = generate_dask_workflow(viper_graph)
    assert len(dask_graph) == 2

    results = dask.compute(dask_graph)[0]
    # Task 0 hit the -1 fallback; task 1 loaded+sub-selected vis[0:2] = 0+1 = 1.0
    assert results[0] == "fallback"
    assert results[1] == 1.0


def test_generate_dask_workflow_single_node_reduce():
    """Line 191 (True branch): ``single_node`` reduce collapses all map nodes
    into one reduce node."""

    def map_fn(input_params):
        return input_params["v"]

    graph = {
        "map": {
            "node_task": map_fn,
            "input_params": [{"v": 1}, {"v": 2}, {"v": 3}, {"v": 4}],
        }
    }

    def reduce_fn(input_data, params):
        assert params == {"scale": 1}
        return params["scale"] * sum(input_data)

    viper_reduce(graph, reduce_fn, {"scale": 1}, mode="single_node")

    node = generate_dask_workflow(graph)
    # single_node collapses to exactly one delayed reduce node.
    assert hasattr(node, "compute")

    result = dask.compute(node)[0]
    assert result == 10


def test_tree_combine_n_nbatch_at_least_two_not_clamped():
    """Branch 35->37 (False side): ``n_batch >= 2`` is used unchanged, combining
    three inputs per reduce node."""
    values = [1, 2, 3, 4, 5, 6, 7]

    node = _tree_combine_n(values, _reduce_sum, None, n_batch=3)
    result = dask.compute(node)[0]
    assert result == sum(values) == 28


def _map_v_graph(values):
    def map_fn(input_params):
        return input_params["v"]

    return {
        "map": {
            "node_task": map_fn,
            "input_params": [{"v": v} for v in values],
        }
    }


def test_generate_dask_workflow_tree_reduce():
    """Line 179 + ``_tree_combine`` (lines 8-20): binary tree reduction over an
    odd number of map nodes (exercises the trailing carried-over node)."""
    graph = _map_v_graph([1, 2, 3, 4, 5])
    viper_reduce(graph, _reduce_sum, None, mode="tree")

    node = generate_dask_workflow(graph)
    assert hasattr(node, "compute")
    assert dask.compute(node)[0] == 15


def test_generate_dask_workflow_tree_n_reduce():
    """Line 185: ``tree_n`` reduce mode dispatches to ``_tree_combine_n`` with the
    recorded ``n_batch``."""
    graph = _map_v_graph([1, 2, 3, 4, 5, 6])
    viper_reduce(graph, _reduce_sum, None, mode="tree_n", n_batch=3)

    node = generate_dask_workflow(graph)
    assert dask.compute(node)[0] == 21


def test_generate_dask_workflow_unknown_reduce_mode_falls_through():
    """Branch 191->198 (False side): a hand-built reduce stage whose mode is none
    of tree/tree_n/single_node leaves the map graph unreduced (falls straight to
    the return)."""

    def map_fn(input_params):
        return input_params["v"]

    graph = {
        "map": {
            "node_task": map_fn,
            "input_params": [{"v": 5}, {"v": 6}],
        },
        # Bypasses reduce() validation on purpose to reach the else fall-through.
        "reduce": {
            "mode": "not_a_real_mode",
            "node_task": lambda d, p: sum(d),
            "input_params": {},
        },
    }

    dask_graph = generate_dask_workflow(graph)
    # No reduce applied: still the list of map nodes.
    assert isinstance(dask_graph, list)
    assert len(dask_graph) == 2

    results = dask.compute(dask_graph)[0]
    assert list(results) == [5, 6]


# --------------------------------------------------------------------------- #
# task_priorities -> per-node dask ``priority`` annotations
# --------------------------------------------------------------------------- #
def _node_priority(node):
    """The dask ``priority`` annotation on a delayed node's own layers, else None."""
    for layer in node.__dask_graph__().layers.values():
        if layer.annotations and "priority" in layer.annotations:
            return layer.annotations["priority"]
    return None


def test_generate_dask_workflow_priority_annotations():
    """``task_priorities`` on the map layer become per-node ``priority``
    annotations (a None entry stays unannotated) without changing results."""
    graph = _map_v_graph([1, 2, 3])
    graph["map"]["task_priorities"] = [0, None, -2]

    nodes = generate_dask_workflow(graph)
    assert [_node_priority(n) for n in nodes] == [0, None, -2]
    assert list(dask.compute(nodes)[0]) == [1, 2, 3]


def test_generate_dask_workflow_priority_annotations_load_graph():
    """Priorities annotate map nodes on both load-graph paths: the
    ``load_node_id == -1`` per-task fallback and the shared-load path (where
    they merge with the ``viper_load_group``/``viper_map_pair`` annotations)."""
    full = _make_full_xds()

    def load_fn(load_params):
        return {"xds_0": full}

    def map_fn(input_params):
        data = input_params["input_data"]
        if data is None:
            return "fallback"
        return float(data["xds_0"]["vis"].values.sum())

    viper_graph = {
        "load": {
            "node_task": load_fn,
            "input_params": [{"marker": "load0"}],
        },
        "map": {
            "node_task": map_fn,
            "input_params": [
                {"input_data": None, "tag": "boundary"},  # load_node_id == -1
                {"tag": "in_chunk"},  # load_node_id == 0
            ],
            "load_node_ids": [-1, 0],
            "relative_data_selections": [
                None,
                {"xds_0": {"frequency": slice(0, 2)}},
            ],
            "task_priorities": [-1, -7],
        },
    }

    nodes = generate_dask_workflow(viper_graph)
    assert [_node_priority(n) for n in nodes] == [-1, -7]
    results = dask.compute(nodes)[0]
    assert results[0] == "fallback"
    assert results[1] == 1.0


def test_generate_dask_workflow_load_node_inherits_max_task_priority():
    """A load node inherits the HIGHEST priority of the map tasks it feeds, so
    unannotated loads (default 0) cannot all outrank negatively-prioritized map
    tasks and run first."""
    full = _make_full_xds()

    def load_fn(load_params):
        return {"xds_0": full}

    def map_fn(input_params):
        return 1

    viper_graph = {
        "load": {
            "node_task": load_fn,
            "input_params": [{"marker": "load0"}],
        },
        "map": {
            "node_task": map_fn,
            "input_params": [{"tag": "a"}, {"tag": "b"}],
            "load_node_ids": [0, 0],
            "relative_data_selections": [
                {"xds_0": {"frequency": slice(0, 2)}},
                {"xds_0": {"frequency": slice(2, 4)}},
            ],
            "task_priorities": [-9, -4],
        },
    }

    nodes = generate_dask_workflow(viper_graph)
    # Map nodes carry their own priorities...
    assert [_node_priority(n) for n in nodes] == [-9, -4]
    # ...and every load-node layer (the load call and its function leaf, seen
    # via both map nodes' graphs) carries max(-9, -4) = -4 -- never the
    # default 0 that would let loads outrank all map tasks.
    load_layer_priorities = [
        layer.annotations.get("priority")
        for n in nodes
        for layer in n.__dask_graph__().layers.values()
        if layer.annotations
        and "viper_load_group" in layer.annotations
        and "viper_map_pair" not in layer.annotations
    ]
    assert load_layer_priorities and set(load_layer_priorities) == {-4}
