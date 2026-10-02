"""Fast unit tests for the per-task memory hooks of make_graph_node_task.

Covers the defaults and every knob of the node-task wrappers (legacy
single-dict and explicit signature) in graphviper.graph_tools.map:

* default: free_memory(collect=True, trim=False) after the task, no
  memory_setup (the mmap threshold is not pinned)
* GRAPHVIPER_PER_TASK_TRIM=1, GRAPHVIPER_PER_TASK_GC=0,
  GRAPHVIPER_MMAP_THRESHOLD=<bytes>, GRAPHVIPER_TASK_MEMORY_MANAGEMENT=0
* the hooks also run when the task raises
* the diagnostics GRAPHVIPER_LOG_MEMORY_STATE=1 and GRAPHVIPER_LOG_CYCLES=1,
  including their never-raise paths

memory_setup and free_memory are replaced by recorders, so no test changes
the allocator of the test process. No network, no Dask cluster.
"""

import importlib

import numpy as np
import pytest

from graphviper.graph_tools.map import make_graph_node_task

# The module itself: graphviper.graph_tools re-exports the map() function under
# the same name, so ``import graphviper.graph_tools.map as m`` binds the function.
gv_map = importlib.import_module("graphviper.graph_tools.map")

_KNOBS = (
    "GRAPHVIPER_TASK_MEMORY_MANAGEMENT",
    "GRAPHVIPER_PER_TASK_GC",
    "GRAPHVIPER_PER_TASK_TRIM",
    "GRAPHVIPER_MMAP_THRESHOLD",
    "GRAPHVIPER_LOG_MEMORY_STATE",
    "GRAPHVIPER_LOG_CYCLES",
)


@pytest.fixture
def hooks(monkeypatch):
    """Clear the knobs and record memory_setup / free_memory calls."""
    import toolviper.utils.memory_management as mm

    for knob in _KNOBS:
        monkeypatch.delenv(knob, raising=False)
    calls = []
    monkeypatch.setattr(
        mm, "memory_setup", lambda threshold: calls.append(("setup", threshold))
    )
    monkeypatch.setattr(
        mm,
        "free_memory",
        lambda collect=True, trim=True: calls.append(("free", collect, trim)),
    )
    return calls


def _legacy(input_params):
    return input_params["a"]


def _explicit(a, b=2):
    return a + b


def _explicit_var_kw(a, **kw):
    return a + len(kw)


# (node task, input_params, expected result)
_TASKS = [
    (_legacy, {"a": 1}, 1),
    (_explicit, {"a": 1, "unused": 0}, 3),
    (_explicit_var_kw, {"a": 1, "x": 0}, 2),
]


@pytest.mark.parametrize(
    "env, expected",
    [
        # default: per-task gc.collect only; no trim, no mmap threshold pin
        ({}, [("free", True, False)]),
        ({"GRAPHVIPER_PER_TASK_TRIM": "1"}, [("free", True, True)]),
        ({"GRAPHVIPER_PER_TASK_TRIM": "0"}, [("free", True, False)]),
        ({"GRAPHVIPER_PER_TASK_GC": "0"}, [("free", False, False)]),
        (
            {"GRAPHVIPER_MMAP_THRESHOLD": "131072"},
            [("setup", 131072), ("free", True, False)],
        ),
        # the behaviour up to graphviper 0.0.52
        (
            {"GRAPHVIPER_MMAP_THRESHOLD": "131072", "GRAPHVIPER_PER_TASK_TRIM": "1"},
            [("setup", 131072), ("free", True, True)],
        ),
        ({"GRAPHVIPER_MMAP_THRESHOLD": "0"}, [("free", True, False)]),
        ({"GRAPHVIPER_MMAP_THRESHOLD": " "}, [("free", True, False)]),
        # the master switch turns every hook off, whatever the finer knobs say
        (
            {
                "GRAPHVIPER_TASK_MEMORY_MANAGEMENT": "0",
                "GRAPHVIPER_PER_TASK_TRIM": "1",
                "GRAPHVIPER_MMAP_THRESHOLD": "131072",
            },
            [],
        ),
    ],
)
@pytest.mark.parametrize("task, params, result", _TASKS)
def test_memory_hooks_defaults_and_knobs(
    hooks, monkeypatch, env, expected, task, params, result
):
    for knob, value in env.items():
        monkeypatch.setenv(knob, value)
    assert make_graph_node_task(task)(dict(params)) == result
    assert hooks == expected


def _legacy_raises(input_params):
    raise RuntimeError("task failed")


def _explicit_raises(a, b=2):
    raise RuntimeError("task failed")


def _explicit_var_kw_raises(a, **kw):
    raise RuntimeError("task failed")


@pytest.mark.parametrize(
    "task", [_legacy_raises, _explicit_raises, _explicit_var_kw_raises]
)
def test_memory_hooks_run_when_the_task_raises(hooks, monkeypatch, task):
    monkeypatch.setenv("GRAPHVIPER_MMAP_THRESHOLD", "65536")
    with pytest.raises(RuntimeError, match="task failed"):
        make_graph_node_task(task)({"a": 1})
    assert hooks == [("setup", 65536), ("free", True, False)]


@pytest.mark.parametrize("value", ["abc", "-1", "1.5e5"])
def test_mmap_threshold_rejects_a_value_that_is_not_a_byte_count(
    hooks, monkeypatch, value
):
    monkeypatch.setenv("GRAPHVIPER_MMAP_THRESHOLD", value)
    ran = []
    with pytest.raises(ValueError, match="GRAPHVIPER_MMAP_THRESHOLD"):
        make_graph_node_task(lambda input_params: ran.append(1))({})
    assert ran == [] and hooks == []


def test_mmap_threshold_bytes(monkeypatch):
    monkeypatch.delenv("GRAPHVIPER_MMAP_THRESHOLD", raising=False)
    assert gv_map._mmap_threshold_bytes() is None
    monkeypatch.setenv("GRAPHVIPER_MMAP_THRESHOLD", "131072")
    assert gv_map._mmap_threshold_bytes() == 131072


# --------------------------------------------------------------------------- #
# Diagnostics: GRAPHVIPER_LOG_MEMORY_STATE / GRAPHVIPER_LOG_CYCLES
# --------------------------------------------------------------------------- #
class _GetFails(dict):
    """input_params whose .get raises, to reach the never-raise branches."""

    def get(self, *args):
        raise RuntimeError("no get")


@pytest.mark.parametrize("task", [_legacy, _explicit])
def test_memory_state_logging(hooks, monkeypatch, capsys, task):
    monkeypatch.setenv("GRAPHVIPER_LOG_MEMORY_STATE", "1")
    monkeypatch.setattr(gv_map, "_memory_setup_logged", False)
    wrapped = make_graph_node_task(task)
    wrapped({"a": 1, "task_id": 4})
    wrapped({"a": 1, "task_id": 5})
    out = capsys.readouterr().out
    assert out.count("graphviper memory-setup") == 1
    assert "graphviper memory-state" in out and "task=5" in out
    # diagnostics never fail the task
    assert wrapped(_GetFails(a=1)) in (1, 3)
    assert "memory-state logging failed" in capsys.readouterr().out


class _Holder:
    pass


@pytest.mark.parametrize("task_kind", ["legacy", "explicit"])
def test_cycle_report(hooks, monkeypatch, capsys, task_kind):
    import gc

    monkeypatch.setenv("GRAPHVIPER_LOG_CYCLES", "1")

    def make_cycle():
        # A reference cycle that holds an ndarray, reached through an
        # attribute, a str dict key, a non-str dict key and a list.
        holder = _Holder()
        holder.payload = {"array": np.zeros(1000), 1: [holder]}
        holder.me = holder

    if task_kind == "legacy":

        def task(input_params):
            make_cycle()
            return input_params["a"]

    else:

        def task(a, task_id=None):
            make_cycle()
            return a

    gc.collect()
    assert make_graph_node_task(task)({"a": 1, "task_id": 9}) == 1
    out = capsys.readouterr().out
    assert "graphviper cycle-report" in out and "task=9" in out
    assert "ndarray_bytes=" in out and "ndarray(" in out
    # diagnostics never fail the task
    assert make_graph_node_task(task)(_GetFails(a=1)) == 1
    assert "cycle-report failed" in capsys.readouterr().out


def test_edge_label_and_holder_path_fallbacks():
    child = object()
    assert gv_map._edge_label({"k": child}, child) == "['k']"
    assert gv_map._edge_label({1: child}, child) == "[<key>]"
    holder = _Holder()
    holder.x = child
    assert gv_map._edge_label(holder, child) == ".x"
    assert gv_map._edge_label([child], child) == "[i]"
    assert gv_map._edge_label(object(), child) == "?"

    class _Raises:
        @property
        def __dict__(self):
            raise RuntimeError("no dict")

    assert gv_map._edge_label(_Raises(), child) == "?"
    # an unhashable/odd target makes the walk fail -> '' instead of raising
    assert gv_map._holder_path(None, None) == ""
    # a target nothing in the dead graph refers to -> just the array
    array = np.zeros(10)
    assert gv_map._holder_path(array, set()).startswith("ndarray(")
