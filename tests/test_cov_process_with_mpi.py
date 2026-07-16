"""Fast, self-contained unit tests for
``graphviper.graph_tools.process_with_mpi``.

mpi4py is NOT installed in this environment, so every test that needs it injects
lightweight fakes into ``sys.modules`` via the ``monkeypatch`` fixture (auto
reverted).  No real MPI, no Dask cluster, no network / measurement sets.
"""

import sys
import types

import pytest

from graphviper.graph_tools.process_with_mpi import (
    _configure_cloudpickle,
    _combine_tree_n_local,
    _combine_tree_n_pool,
    processes_with_mpi,
)
from graphviper.graph_tools.map import map as viper_map
from graphviper.graph_tools.reduce import reduce as viper_reduce


# --------------------------------------------------------------------------- #
# Fakes / helpers
# --------------------------------------------------------------------------- #
class FakeFuture:
    """Eager future: computes immediately, ``.result()`` returns the value."""

    def __init__(self, value):
        self._value = value

    def result(self):
        return self._value


class FakeExecutor:
    """Synchronous stand-in for ``MPIPoolExecutor`` (context manager)."""

    def __init__(self, max_workers=None):
        self.max_workers = max_workers
        self.entered = False
        self.exited = False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, *exc):
        self.exited = True
        return False

    def map(self, fn, iterable, chunksize=1):
        self.last_chunksize = chunksize
        return [fn(x) for x in iterable]

    def submit(self, fn, *args):
        return FakeFuture(fn(*args))


class FakePickle:
    """Mimics ``MPI.pickle``: re-``__init__`` swaps the (dumps, loads) pair."""

    def __init__(self, dumps=None, loads=None):
        self.dumps = dumps
        self.loads = loads


def install_fake_mpi(monkeypatch, world_size=2, thread_level=2, query_raises=False):
    """Inject fake ``mpi4py`` / ``mpi4py.MPI`` / ``mpi4py.futures`` modules.

    Returns ``(mpi4py, MPI, futures)`` so a test can inspect them afterwards.
    """
    mpi4py = types.ModuleType("mpi4py")
    MPI = types.ModuleType("mpi4py.MPI")

    class Comm:
        def Get_size(self):
            return world_size

    MPI.COMM_WORLD = Comm()
    MPI.THREAD_SERIALIZED = 2

    if query_raises:

        def _query_thread():
            raise RuntimeError("query_thread unavailable")

    else:

        def _query_thread():
            return thread_level

    MPI.Query_thread = _query_thread
    MPI.pickle = FakePickle()

    futures = types.ModuleType("mpi4py.futures")
    # Record the executors that get constructed so a test can verify max_workers.
    futures.constructed = []

    def _make_executor(max_workers=None):
        ex = FakeExecutor(max_workers=max_workers)
        futures.constructed.append(ex)
        return ex

    futures.MPIPoolExecutor = _make_executor

    mpi4py.MPI = MPI
    mpi4py.futures = futures

    monkeypatch.setitem(sys.modules, "mpi4py", mpi4py)
    monkeypatch.setitem(sys.modules, "mpi4py.MPI", MPI)
    monkeypatch.setitem(sys.modules, "mpi4py.futures", futures)
    return mpi4py, MPI, futures


# Module-level node / reduce tasks (return stdlib types).
def _map_node_task(input_params):
    """Legacy single-dict node task -> returned unchanged by map()."""
    return input_params["v"]


def _reduce_sum(input_data, input_params):
    """Reduce task: sum a batch/list of ints."""
    return sum(input_data)


def build_map_graph(values):
    """Build a real map graph (via graphviper.map) over ``values``."""
    mapping = {i: {"v": val} for i, val in enumerate(values)}
    return viper_map(
        input_data={},
        node_task_data_mapping=mapping,
        node_task=_map_node_task,
        input_params={},
    )


# --------------------------------------------------------------------------- #
# _configure_cloudpickle  (lines 90-102)
# --------------------------------------------------------------------------- #
def test_configure_cloudpickle_success(monkeypatch):
    import cloudpickle

    _, MPI, _ = install_fake_mpi(monkeypatch)
    assert _configure_cloudpickle() is True
    # MPI.pickle.__init__(cloudpickle.dumps, cloudpickle.loads) was called.
    assert MPI.pickle.dumps is cloudpickle.dumps
    assert MPI.pickle.loads is cloudpickle.loads


def test_configure_cloudpickle_failure(monkeypatch):
    # `import cloudpickle` raises ImportError when the module is None in sys.modules.
    monkeypatch.setitem(sys.modules, "cloudpickle", None)
    # No exception should escape; returns False.
    assert _configure_cloudpickle() is False


# --------------------------------------------------------------------------- #
# _combine_tree_n_local  (existing coverage) + explicit here for completeness
# --------------------------------------------------------------------------- #
def test_combine_tree_n_local_arities_and_clamp():
    results = [1, 2, 3, 4, 5]
    expected = sum(results)
    for n_batch in (1, 2, 3, 5, 10):  # 1 -> clamp to 2
        assert _combine_tree_n_local(results, _reduce_sum, {}, n_batch) == expected
    # single element: returned as-is, reduce task never called.
    assert _combine_tree_n_local([42], _reduce_sum, {}, 2) == 42


# --------------------------------------------------------------------------- #
# _combine_tree_n_pool  (lines 137-151)
# --------------------------------------------------------------------------- #
def test_combine_tree_n_pool_single_element_short_circuits():
    # len(results) == 1: while loop is skipped and items[0] is returned (line 151).
    ex = FakeExecutor()
    assert _combine_tree_n_pool(ex, [99], _reduce_sum, {}, 2) == 99


def test_combine_tree_n_pool_multi_element_reduces():
    # n_batch=1 -> clamp to 2 (covers 137-138); 3 items -> a real batch + a
    # trailing singleton (covers both for-loop branches) then a second layer.
    # Each submitted batch resolves via the future's .result() (line 150).
    ex = FakeExecutor()
    assert _combine_tree_n_pool(ex, [1, 2, 3], _reduce_sum, {}, 1) == 6


# --------------------------------------------------------------------------- #
# processes_with_mpi  (lines 192-297)
# --------------------------------------------------------------------------- #
def test_processes_with_mpi_no_reduce_defaults(monkeypatch):
    # cluster_setup=None -> uses {} defaults (use_cloudpickle True by default).
    _, MPI, futures = install_fake_mpi(monkeypatch, world_size=4)
    graph = build_map_graph([10, 20, 30])
    result = processes_with_mpi(graph)  # cluster_setup defaulted to None
    assert result == [10, 20, 30]
    # cloudpickle was configured (use_cloudpickle defaults True).
    import cloudpickle

    assert MPI.pickle.dumps is cloudpickle.dumps
    # Executor was constructed with the default max_workers=None and used as a CM.
    assert futures.constructed[0].max_workers is None
    assert futures.constructed[0].entered and futures.constructed[0].exited


def test_processes_with_mpi_world_size_one_warns(monkeypatch, caplog):
    install_fake_mpi(monkeypatch, world_size=1)
    graph = build_map_graph([1, 2])
    result = processes_with_mpi(graph, {"use_cloudpickle": False})
    assert result == [1, 2]


def test_processes_with_mpi_thread_level_low_warns(monkeypatch):
    # thread_level (1) < THREAD_SERIALIZED (2) -> warning branch.
    install_fake_mpi(monkeypatch, world_size=2, thread_level=1)
    graph = build_map_graph([5])
    assert processes_with_mpi(graph, {"use_cloudpickle": False}) == [5]


def test_processes_with_mpi_thread_level_ok_info(monkeypatch):
    # thread_level (3) >= THREAD_SERIALIZED (2) -> info branch.
    install_fake_mpi(monkeypatch, world_size=2, thread_level=3)
    graph = build_map_graph([7])
    assert processes_with_mpi(graph, {"use_cloudpickle": False}) == [7]


def test_processes_with_mpi_query_thread_raises(monkeypatch):
    # Query_thread raising -> the bare `except: pass` branch.
    install_fake_mpi(monkeypatch, world_size=2, query_raises=True)
    graph = build_map_graph([3, 4])
    assert processes_with_mpi(graph, {"use_cloudpickle": False}) == [3, 4]


def test_processes_with_mpi_progress_every(monkeypatch):
    install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2, 3, 4, 5])
    # progress_every=2 exercises the enumerate/progress-log branch, incl. the
    # final-task (i == n_tasks) log for the odd count.
    result = processes_with_mpi(graph, {"use_cloudpickle": False, "progress_every": 2})
    assert result == [1, 2, 3, 4, 5]


def test_processes_with_mpi_chunksize_forwarded(monkeypatch):
    _, _, futures = install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2])
    processes_with_mpi(graph, {"use_cloudpickle": False, "chunksize": 7})
    assert futures.constructed[0].last_chunksize == 7


def test_processes_with_mpi_load_stage_warns(monkeypatch):
    install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2, 3])
    graph["load"] = {"node_task": None, "input_params": []}  # trigger warning branch
    assert processes_with_mpi(graph, {"use_cloudpickle": False}) == [1, 2, 3]


def test_processes_with_mpi_reduce_single_node_local(monkeypatch):
    install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2, 3, 4])
    graph = viper_reduce(graph, _reduce_sum, {}, mode="single_node")
    assert processes_with_mpi(graph, {"use_cloudpickle": False}) == 10


def test_processes_with_mpi_reduce_single_node_in_pool(monkeypatch):
    install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2, 3, 4])
    graph = viper_reduce(graph, _reduce_sum, {}, mode="single_node")
    assert (
        processes_with_mpi(graph, {"use_cloudpickle": False, "reduce_in_pool": True})
        == 10
    )


def test_processes_with_mpi_reduce_tree_local(monkeypatch):
    install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2, 3, 4, 5])
    graph = viper_reduce(graph, _reduce_sum, {}, mode="tree")
    assert processes_with_mpi(graph, {"use_cloudpickle": False}) == 15


def test_processes_with_mpi_reduce_tree_in_pool(monkeypatch):
    # reduce_in_pool tree path routes through _combine_tree_n_pool; each batch is
    # submitted to the (fake) pool and resolved via its future's .result().
    install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2, 3, 4, 5])
    graph = viper_reduce(graph, _reduce_sum, {}, mode="tree")
    assert (
        processes_with_mpi(graph, {"use_cloudpickle": False, "reduce_in_pool": True})
        == 15
    )


def test_processes_with_mpi_reduce_tree_in_pool_single_result(monkeypatch):
    # A single map result makes _combine_tree_n_pool short-circuit (no future),
    # so the reduce_in_pool tree path returns cleanly (reaches line 151).
    install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([42])
    graph = viper_reduce(graph, _reduce_sum, {}, mode="tree")
    assert (
        processes_with_mpi(graph, {"use_cloudpickle": False, "reduce_in_pool": True})
        == 42
    )


def test_processes_with_mpi_reduce_tree_n_local(monkeypatch):
    install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2, 3, 4, 5, 6, 7])
    graph = viper_reduce(graph, _reduce_sum, {}, mode="tree_n", n_batch=3)
    assert processes_with_mpi(graph, {"use_cloudpickle": False}) == 28


def test_processes_with_mpi_reduce_tree_n_in_pool(monkeypatch):
    # tree_n reduce_in_pool path with >1 results, resolved via the pool futures.
    install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2, 3, 4, 5, 6, 7])
    graph = viper_reduce(graph, _reduce_sum, {}, mode="tree_n", n_batch=3)
    assert (
        processes_with_mpi(graph, {"use_cloudpickle": False, "reduce_in_pool": True})
        == 28
    )


def test_processes_with_mpi_reduce_unknown_mode_raises(monkeypatch):
    install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2, 3])
    graph = viper_reduce(graph, _reduce_sum, {}, mode="single_node")
    graph["reduce"]["mode"] = "bogus"  # bypass reduce()'s own validation
    with pytest.raises(ValueError, match="Unknown reduce mode 'bogus'"):
        processes_with_mpi(graph, {"use_cloudpickle": False})


def test_processes_with_mpi_use_cloudpickle_true(monkeypatch):
    # Explicitly drive the use_cloudpickle=True branch with a fake MPI.pickle.
    _, MPI, _ = install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2])
    result = processes_with_mpi(graph, {"use_cloudpickle": True})
    assert result == [1, 2]
    import cloudpickle

    assert MPI.pickle.dumps is cloudpickle.dumps


# --------------------------------------------------------------------------- #
# Teardown watchdog (force_exit_after / teardown_force_exit_seconds)
# --------------------------------------------------------------------------- #
def test_force_exit_after_arms_daemon_timer():
    import graphviper.graph_tools.process_with_mpi as pwm

    timer = pwm.force_exit_after(3600)
    try:
        assert timer is not None
        assert timer.daemon is True
        assert timer.is_alive()
        assert pwm._teardown_watchdog is timer
    finally:
        pwm.force_exit_after(None)  # disarm
    assert pwm._teardown_watchdog is None
    timer.join(timeout=5)  # cancel() is async: the thread exits on wakeup
    assert not timer.is_alive()


def test_force_exit_after_rearm_cancels_previous():
    import graphviper.graph_tools.process_with_mpi as pwm

    first = pwm.force_exit_after(3600)
    second = pwm.force_exit_after(3600)
    try:
        assert second is not first
        first.join(timeout=5)  # cancel() is async: the thread exits on wakeup
        assert not first.is_alive()  # cancelled by the re-arm
        assert second.is_alive()
    finally:
        pwm.force_exit_after(None)


def test_force_exit_after_fire_calls_os_exit(monkeypatch):
    # Invoke the timer's payload directly (never let a real timer fire) and
    # verify it hard-exits with code 0.
    import graphviper.graph_tools.process_with_mpi as pwm
    import os

    calls = []
    monkeypatch.setattr(os, "_exit", lambda code: calls.append(code))
    timer = pwm.force_exit_after(3600, note="unit test")
    try:
        timer.function()
    finally:
        pwm.force_exit_after(None)
    assert calls == [0]


def test_processes_with_mpi_arms_teardown_watchdog(monkeypatch):
    import graphviper.graph_tools.process_with_mpi as pwm

    install_fake_mpi(monkeypatch, world_size=2)
    armed = []
    monkeypatch.setattr(
        pwm, "force_exit_after", lambda seconds, note="": armed.append(seconds)
    )
    graph = build_map_graph([1, 2])
    result = processes_with_mpi(
        graph, {"use_cloudpickle": False, "teardown_force_exit_seconds": 300}
    )
    assert result == [1, 2]
    assert armed == [300]


def test_processes_with_mpi_watchdog_off_by_default(monkeypatch):
    import graphviper.graph_tools.process_with_mpi as pwm

    install_fake_mpi(monkeypatch, world_size=2)
    armed = []
    monkeypatch.setattr(
        pwm, "force_exit_after", lambda seconds, note="": armed.append(seconds)
    )
    assert processes_with_mpi(build_map_graph([1]), {"use_cloudpickle": False}) == [1]
    assert armed == []


def test_processes_with_mpi_task_priorities_reorders_dispatch(monkeypatch):
    """``task_priorities`` -> dispatch order is the priority sort (higher first,
    ties broken by task_id, None -> 0) while the RESULTS come back in input
    order (the inverse permutation restores the documented contract)."""
    install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([10, 20, 30, 40])
    graph["map"]["task_priorities"] = [0, -3, None, -1]

    dispatched = []
    inner_fn = graph["map"]["node_task"]

    def recording_fn(input_params):
        dispatched.append(input_params["v"])
        return inner_fn(input_params)

    graph["map"]["node_task"] = recording_fn
    result = processes_with_mpi(graph, {"use_cloudpickle": False})
    # Sort key (-priority, task_id): task0 (0), task2 (None->0), task3, task1.
    assert dispatched == [10, 30, 40, 20]
    assert result == [10, 20, 30, 40]


def test_processes_with_mpi_task_priorities_progress_path(monkeypatch):
    """The inverse permutation also runs on the progress_every branch."""
    install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2, 3])
    graph["map"]["task_priorities"] = [-2, -1, 0]
    result = processes_with_mpi(graph, {"use_cloudpickle": False, "progress_every": 2})
    assert result == [1, 2, 3]


def test_processes_with_mpi_task_priorities_with_reduce(monkeypatch):
    """Reduce over priority-permuted map tasks still combines input-order
    results (the adjacent-batch tree relies on the inverse permutation)."""
    install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2, 3, 4, 5])
    graph["map"]["task_priorities"] = [-4, -3, -2, -1, 0]
    graph = viper_reduce(graph, _reduce_sum, {}, mode="tree_n", n_batch=3)
    assert processes_with_mpi(graph, {"use_cloudpickle": False}) == 15


def test_processes_with_mpi_task_priorities_forces_chunksize_one(monkeypatch):
    """chunksize>1 with priorities would hand each worker a consecutive block
    of the priority order (defeating the interleaving) -> forced to 1."""
    _, _, futures = install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2, 3])
    graph["map"]["task_priorities"] = [0, -1, -2]
    result = processes_with_mpi(graph, {"use_cloudpickle": False, "chunksize": 8})
    assert result == [1, 2, 3]
    assert futures.constructed[0].last_chunksize == 1


# --------------------------------------------------------------------------- #
# append (post-reduce node) on the MPI backend
# --------------------------------------------------------------------------- #
def _append_scale(input_data, input_params):
    return input_data * input_params["factor"]


def _append_shift(input_data, input_params):
    return input_data + input_params["shift"]


def test_processes_with_mpi_append_manager_local(monkeypatch):
    """Appended nodes run on the manager after the reduce (default
    reduce_in_pool=False), chained in call order: (10 + 1) * 2 = 22."""
    from graphviper.graph_tools.append import append as viper_append

    install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2, 3, 4])
    graph = viper_reduce(graph, _reduce_sum, {}, mode="tree")
    viper_append(graph, _append_shift, {"shift": 1})
    viper_append(graph, _append_scale, {"factor": 2})

    assert processes_with_mpi(graph, {"use_cloudpickle": False}) == 22


def test_processes_with_mpi_append_in_pool(monkeypatch):
    """With reduce_in_pool=True the appended node is executor.submit()ted."""
    from graphviper.graph_tools.append import append as viper_append

    _, _, futures = install_fake_mpi(monkeypatch, world_size=2)
    graph = build_map_graph([1, 2, 3, 4])
    graph = viper_reduce(graph, _reduce_sum, {}, mode="single_node")
    viper_append(graph, _append_scale, {"factor": 10})

    result = processes_with_mpi(
        graph, {"use_cloudpickle": False, "reduce_in_pool": True}
    )
    assert result == 100
