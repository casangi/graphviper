"""Additional fast unit tests for graphviper.graph_tools.map, targeting the
lines not exercised by tests/test_graph_tools.py:

* 192      map(in_memory_compute=True) -> _select_data (dict input)
* 199-202  map() local_cache branch (date_time / viper_local_dir / node_ip)
* 212->214 map() load stage with load_node_input_params left as None
* 322-323  _build_load_stage: a data_selection dim NOT in disk_chunk_sizes
* 352-360  _select_data for both a plain dict and an xr.DataTree
* 363-406  _local_cache_configuration (+/- VIPER_LOCAL_DIR) and
           _get_unique_resource_ip (success + assert failure)

No network, no real Dask cluster, no measurement sets: synthetic xarray only.
"""

import numpy as np
import pytest
import xarray as xr

from graphviper.graph_tools.map import (
    map as viper_map,
    _select_data,
    _local_cache_configuration,
    _get_unique_resource_ip,
    _build_load_stage,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _make_xds(n_freq=6):
    """Small synthetic dataset: vis[i] = i along frequency."""
    freq_vals = np.arange(n_freq, dtype=float)
    return xr.Dataset(
        {"vis": (["frequency"], freq_vals.copy())},
        coords={"frequency": freq_vals},
    )


class _FakeClient:
    """Stand-in for a Dask distributed client exposing scheduler_info()."""

    def __init__(self, workers_info):
        self._workers_info = workers_info

    def scheduler_info(self):
        return {"workers": self._workers_info}


def _workers_info(ips):
    """Build a scheduler workers dict; each worker is annotated with its own ip
    resource so _get_unique_resource_ip's assert passes.  ``ips`` may repeat to
    model several workers per physical node."""
    info = {}
    for i, ip in enumerate(ips):
        worker_addr = f"tcp://{ip}:{5000 + i}"
        info[worker_addr] = {"resources": {ip: 1.0}}
    return info


# --------------------------------------------------------------------------- #
# _select_data  (lines 352-360)   +   map in_memory_compute path (line 192)
# --------------------------------------------------------------------------- #
def test_select_data_dict():
    """_select_data on a plain dict returns a dict of loaded isel selections."""
    xds = _make_xds(6)
    input_data = {"xds_0": xds}
    data_selection = {"xds_0": {"frequency": slice(2, 5)}}

    out = _select_data(input_data, data_selection)

    assert isinstance(out, dict)
    assert set(out) == {"xds_0"}
    # frequencies 2,3,4 selected -> vis == [2,3,4]
    np.testing.assert_array_equal(out["xds_0"]["vis"].values, [2.0, 3.0, 4.0])
    # .load() means data is materialized (not dask-backed)
    assert not hasattr(out["xds_0"]["vis"].data, "dask")


def test_select_data_datatree():
    """_select_data on an xr.DataTree takes the DataTree branch and returns a
    DataTree of loaded selections."""
    xds = _make_xds(6)
    tree = xr.DataTree.from_dict({"xds_0": xds})
    data_selection = {"xds_0": {"frequency": slice(1, 3)}}

    out = _select_data(tree, data_selection)

    assert isinstance(out, xr.DataTree)
    np.testing.assert_array_equal(out["xds_0"]["vis"].values, [1.0, 2.0])


def test_map_in_memory_compute_selects_data():
    """map(in_memory_compute=True) loads the per-task selection into
    input_params['input_data'] via _select_data (line 192)."""
    xds = _make_xds(6)
    input_data = {"xds_0": xds}

    node_task_data_mapping = {
        0: {
            "chunk_indices": (0,),
            "parallel_dims": ["frequency"],
            "data_selection": {"xds_0": {"frequency": slice(0, 3)}},
            "task_coords": {},
        },
        1: {
            "chunk_indices": (1,),
            "parallel_dims": ["frequency"],
            "data_selection": {"xds_0": {"frequency": slice(3, 6)}},
            "task_coords": {},
        },
    }

    def node(input_params):
        return input_params["input_data"]

    graph = viper_map(
        input_data=input_data,
        node_task_data_mapping=node_task_data_mapping,
        node_task=node,
        input_params={},
        in_memory_compute=True,
        client=None,
    )

    ips = graph["map"]["input_params"]
    assert ips[0]["input_data"] is not None
    np.testing.assert_array_equal(
        ips[0]["input_data"]["xds_0"]["vis"].values, [0.0, 1.0, 2.0]
    )
    np.testing.assert_array_equal(
        ips[1]["input_data"]["xds_0"]["vis"].values, [3.0, 4.0, 5.0]
    )
    # in_memory_compute path does not set the local-cache annotations.
    assert ips[0]["date_time"] is None
    assert "viper_local_dir" not in ips[0]


# --------------------------------------------------------------------------- #
# _get_unique_resource_ip  (lines 396-406)
# --------------------------------------------------------------------------- #
def test_get_unique_resource_ip_dedupes():
    """Two workers on the same ip collapse to one node; distinct ips stay
    separate, in first-seen order."""
    info = _workers_info(["127.0.0.1", "127.0.0.1", "127.0.0.2"])
    assert _get_unique_resource_ip(info) == ["127.0.0.1", "127.0.0.2"]


def test_get_unique_resource_ip_asserts_when_not_annotated():
    """A worker whose resources lack its own ip triggers the AssertionError."""
    info = {"tcp://127.0.0.1:5000": {"resources": {"slots": 1}}}
    with pytest.raises(AssertionError, match="not been annotated"):
        _get_unique_resource_ip(info)


# --------------------------------------------------------------------------- #
# _local_cache_configuration  (lines 363-393)
# --------------------------------------------------------------------------- #
def test_local_cache_configuration_disabled(monkeypatch):
    """Without VIPER_LOCAL_DIR the function short-circuits to all-None/False."""
    monkeypatch.delenv("VIPER_LOCAL_DIR", raising=False)

    result = _local_cache_configuration(n_tasks=4, client=None, date_time=None)

    assert result == (False, None, None, None, None)


def test_local_cache_configuration_enabled_generates_date_time(monkeypatch, tmp_path):
    """With VIPER_LOCAL_DIR set and date_time None, a date_time is generated and
    the task->node map is built (tasks_per_compute_node path, no padding)."""
    monkeypatch.setenv("VIPER_LOCAL_DIR", str(tmp_path))
    client = _FakeClient(_workers_info(["127.0.0.1", "127.0.0.2"]))

    (
        local_cache,
        viper_local_dir,
        date_time,
        tasks_to_node_map,
        nodes_ip_list,
    ) = _local_cache_configuration(n_tasks=4, client=client, date_time=None)

    assert local_cache is True
    assert viper_local_dir == str(tmp_path)
    assert isinstance(date_time, str) and len(date_time) == 12  # yymmddHHMMSS
    assert nodes_ip_list == ["127.0.0.1", "127.0.0.2"]
    # 4 tasks / 2 nodes -> 2 per node, exactly covers 4 tasks (no padding).
    np.testing.assert_array_equal(tasks_to_node_map, [0, 0, 1, 1])


def test_local_cache_configuration_keeps_supplied_date_time(monkeypatch, tmp_path):
    """A caller-supplied date_time is preserved (the `if date_time is None`
    branch is not taken)."""
    monkeypatch.setenv("VIPER_LOCAL_DIR", str(tmp_path))
    client = _FakeClient(_workers_info(["10.0.0.1"]))

    _, _, date_time, _, _ = _local_cache_configuration(
        n_tasks=2, client=client, date_time="240101000000"
    )
    assert date_time == "240101000000"


def test_local_cache_configuration_zero_tasks_per_node(monkeypatch, tmp_path):
    """n_tasks < n_nodes forces tasks_per_compute_node == 0 -> clamped to 1."""
    monkeypatch.setenv("VIPER_LOCAL_DIR", str(tmp_path))
    client = _FakeClient(_workers_info(["10.0.0.1", "10.0.0.2", "10.0.0.3"]))

    _, _, _, tasks_to_node_map, nodes_ip_list = _local_cache_configuration(
        n_tasks=1, client=client, date_time="x" * 12
    )
    # floor(1/3 + 0.5) == 0 -> clamped to 1 -> one task slot per node.
    assert len(nodes_ip_list) == 3
    np.testing.assert_array_equal(tasks_to_node_map, [0, 1, 2])


def test_local_cache_configuration_padding(monkeypatch, tmp_path):
    """When repeat() yields fewer slots than n_tasks, the tail is padded with the
    last node id (lines 387-391)."""
    monkeypatch.setenv("VIPER_LOCAL_DIR", str(tmp_path))
    client = _FakeClient(_workers_info(["10.0.0.1", "10.0.0.2", "10.0.0.3"]))

    # floor(7/3 + 0.5) == floor(2.833) == 2 -> repeat gives len 6 < 7 -> pad 1.
    _, _, _, tasks_to_node_map, _ = _local_cache_configuration(
        n_tasks=7, client=client, date_time="x" * 12
    )
    assert len(tasks_to_node_map) == 7
    np.testing.assert_array_equal(tasks_to_node_map, [0, 0, 1, 1, 2, 2, 2])


# --------------------------------------------------------------------------- #
# map() local_cache branch  (lines 199-202)
# --------------------------------------------------------------------------- #
def test_map_local_cache_annotations(monkeypatch, tmp_path):
    """map() with a client and VIPER_LOCAL_DIR set injects date_time,
    viper_local_dir and node_ip into every task's input_params."""
    monkeypatch.setenv("VIPER_LOCAL_DIR", str(tmp_path))
    client = _FakeClient(_workers_info(["127.0.0.1", "127.0.0.2"]))

    node_task_data_mapping = {
        0: {
            "chunk_indices": (0,),
            "parallel_dims": [],
            "data_selection": {},
            "task_coords": {},
        },
        1: {
            "chunk_indices": (1,),
            "parallel_dims": [],
            "data_selection": {},
            "task_coords": {},
        },
    }

    def node(input_params):
        return input_params["node_ip"]

    graph = viper_map(
        input_data={},
        node_task_data_mapping=node_task_data_mapping,
        node_task=node,
        input_params={},
        in_memory_compute=False,
        client=client,
        date_time="240101000000",
    )

    ips = graph["map"]["input_params"]
    assert ips[0]["viper_local_dir"] == str(tmp_path)
    assert ips[0]["date_time"] == "240101000000"
    # 2 tasks / 2 nodes -> task 0 on node 0, task 1 on node 1.
    assert ips[0]["node_ip"] == "127.0.0.1"
    assert ips[1]["node_ip"] == "127.0.0.2"
    assert ips[0]["input_data"] is None


# --------------------------------------------------------------------------- #
# map() load stage with load_node_input_params=None  (line 212->214)
# --------------------------------------------------------------------------- #
def test_map_load_stage_default_load_node_input_params():
    """map(..., load_node_input_params=None) still builds a 'load' stage,
    defaulting the extra-params dict to {} (line 212-213)."""

    node_task_data_mapping = {
        0: {
            "chunk_indices": (0,),
            "parallel_dims": ["frequency"],
            "data_selection": {"xds_0": {"frequency": slice(0, 2)}},
            "task_coords": {},
        },
        1: {
            "chunk_indices": (1,),
            "parallel_dims": ["frequency"],
            "data_selection": {"xds_0": {"frequency": slice(2, 4)}},
            "task_coords": {},
        },
    }

    def node(input_params):
        return input_params

    def load_fn(load_params):
        return {}

    graph = viper_map(
        input_data={},
        node_task_data_mapping=node_task_data_mapping,
        node_task=node,
        input_params={"input_data_store": "synthetic"},
        in_memory_compute=False,
        data_loading_task=load_fn,
        disk_chunk_sizes={"frequency": 4},
        load_node_input_params=None,
    )

    assert "load" in graph
    # Both 2-channel tasks fall inside disk chunk [0,4) -> one shared load node.
    assert len(graph["load"]["input_params"]) == 1
    assert graph["map"]["load_node_ids"] == [0, 0]
    # No extra params were merged (defaulted to {}).
    assert set(graph["load"]["input_params"][0]) == {
        "input_data_store",
        "data_selection",
    }


def test_map_load_stage_with_load_node_input_params():
    """map(..., load_node_input_params={...}) takes the not-None branch (212->214):
    the supplied dict is merged into each load node's params."""

    node_task_data_mapping = {
        0: {
            "chunk_indices": (0,),
            "parallel_dims": ["frequency"],
            "data_selection": {"xds_0": {"frequency": slice(0, 2)}},
            "task_coords": {},
        },
    }

    def node(input_params):
        return input_params

    def load_fn(load_params):
        return {}

    graph = viper_map(
        input_data={},
        node_task_data_mapping=node_task_data_mapping,
        node_task=node,
        input_params={"input_data_store": "synthetic"},
        in_memory_compute=False,
        data_loading_task=load_fn,
        disk_chunk_sizes={"frequency": 4},
        load_node_input_params={"data_group_name": "corrected"},
    )

    assert graph["load"]["input_params"][0]["data_group_name"] == "corrected"


# --------------------------------------------------------------------------- #
# _build_load_stage non-disk dim else branch  (lines 322-323)
# --------------------------------------------------------------------------- #
def test_build_load_stage_non_disk_dim_keeps_original_slice():
    """A data_selection dim that is NOT in disk_chunk_sizes takes the else branch:
    disk-level selection is slice(None) and the relative selection keeps the
    original slice."""
    input_param_list = [
        {
            "input_data_store": "fake_store",
            "data_selection": {
                "xds_0": {
                    "frequency": slice(0, 5),  # disk-chunked
                    "time": slice(2, 8),  # NOT disk-chunked -> else branch
                }
            },
        }
    ]

    load_params_list, load_node_ids, relative_sels = _build_load_stage(
        input_param_list,
        disk_chunk_sizes={"frequency": 5},
        load_node_input_params={},
    )

    assert load_node_ids == [0]
    disk_sel = load_params_list[0]["data_selection"]["xds_0"]
    # frequency disk-chunked to full chunk [0,5); time loads everything.
    assert disk_sel["frequency"] == slice(0, 5)
    assert disk_sel["time"] == slice(None)
    # relative selection keeps the original absolute slice for the non-disk dim.
    assert relative_sels[0]["xds_0"]["time"] == slice(2, 8)
    assert relative_sels[0]["xds_0"]["frequency"] == slice(0, 5)


# --------------------------------------------------------------------------- #
# monitor_node_task / map(monitor_resources_seconds=...)
# --------------------------------------------------------------------------- #
def test_monitor_node_task_attaches_series():
    from graphviper.graph_tools.map import monitor_node_task
    import pickle
    import time

    def node(input_params):
        time.sleep(0.08)
        return {"task_id": input_params["task_id"]}

    out = monitor_node_task(node, 0.02)({"task_id": 3})
    assert out["task_id"] == 3  # original payload intact
    usage = out["resource_usage"]
    assert usage["sample_interval_seconds"] == 0.02
    assert usage["start_unixtime"] > 1e9  # wall-clock anchor for run timelines
    n = len(usage["time_seconds"])
    assert n >= 1
    assert len(usage["cpu_percent"]) == n
    assert len(usage["memory_rss_bytes"]) == n
    assert all(b > 0 for b in usage["memory_rss_bytes"])
    # MPI worker->manager results must be stdlib-pickle-serialisable.
    pickle.dumps(out)


def test_monitor_node_task_short_task_gets_final_sample():
    from graphviper.graph_tools.map import monitor_node_task

    out = monitor_node_task(lambda p: {"ok": 1}, 60.0)({})
    # initial sample + the final sample taken on stop, despite interval >> runtime
    assert len(out["resource_usage"]["time_seconds"]) >= 1
    assert out["ok"] == 1


def test_monitor_node_task_non_dict_passthrough():
    from graphviper.graph_tools.map import monitor_node_task

    assert monitor_node_task(lambda p: [1, 2, 3], 0.02)({}) == [1, 2, 3]


def test_monitor_node_task_without_psutil_runs_unmonitored(monkeypatch):
    import sys
    from graphviper.graph_tools.map import monitor_node_task

    # `import psutil` raises ImportError when the module is None in sys.modules.
    monkeypatch.setitem(sys.modules, "psutil", None)
    out = monitor_node_task(lambda p: {"ok": 1}, 0.02)({})
    assert out == {"ok": 1}  # no resource_usage key, task result untouched


def test_map_monitor_resources_wraps_node_task():
    import time

    xds = _make_xds(3)
    node_task_data_mapping = {
        0: {
            "chunk_indices": (0,),
            "parallel_dims": ["frequency"],
            "data_selection": {"xds_0": {"frequency": slice(0, 3)}},
            "task_coords": {},
        },
    }

    def node(input_params):
        time.sleep(0.05)
        return {"task_id": input_params["task_id"]}

    graph = viper_map(
        input_data={"xds_0": xds},
        node_task_data_mapping=node_task_data_mapping,
        node_task=node,
        input_params={},
        in_memory_compute=True,
        client=None,
        monitor_resources_seconds=0.02,
    )
    assert graph["map"]["node_task"].__name__.endswith("_monitored")
    out = graph["map"]["node_task"](graph["map"]["input_params"][0])
    assert out["task_id"] == 0
    assert len(out["resource_usage"]["time_seconds"]) >= 1


def test_map_monitor_resources_off_by_default():
    xds = _make_xds(3)
    node_task_data_mapping = {
        0: {
            "chunk_indices": (0,),
            "parallel_dims": ["frequency"],
            "data_selection": {"xds_0": {"frequency": slice(0, 3)}},
            "task_coords": {},
        },
    }

    def node(input_params):
        return {"task_id": input_params["task_id"]}

    graph = viper_map(
        input_data={"xds_0": xds},
        node_task_data_mapping=node_task_data_mapping,
        node_task=node,
        input_params={},
        in_memory_compute=True,
        client=None,
    )
    assert not graph["map"]["node_task"].__name__.endswith("_monitored")
    assert "resource_usage" not in graph["map"]["node_task"](
        graph["map"]["input_params"][0]
    )


# --------------------------------------------------------------------------- #
# map(task_priorities=...)
# --------------------------------------------------------------------------- #
def _priority_node_task(input_params):
    return input_params["v"]


def test_map_task_priorities_stored_aligned():
    """``task_priorities`` -> ``graph["map"]["task_priorities"]`` list aligned
    with ``input_params`` (task_id order); ids missing from the dict -> None."""
    mapping = {i: {"v": 10 * i} for i in range(3)}
    graph = viper_map(
        input_data={},
        node_task_data_mapping=mapping,
        node_task=_priority_node_task,
        input_params={},
        task_priorities={0: 0, 2: -5},
    )
    assert graph["map"]["task_priorities"] == [0, None, -5]
    # Same order as the per-task parameter dicts.
    assert [p["task_id"] for p in graph["map"]["input_params"]] == [0, 1, 2]


def test_map_task_priorities_default_absent():
    """No ``task_priorities`` -> the graph carries no key at all (backends
    treat absence as 'no reordering')."""
    mapping = {i: {"v": i} for i in range(2)}
    graph = viper_map(
        input_data={},
        node_task_data_mapping=mapping,
        node_task=_priority_node_task,
        input_params={},
    )
    assert "task_priorities" not in graph["map"]
