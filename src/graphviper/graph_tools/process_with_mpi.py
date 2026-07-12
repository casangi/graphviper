"""MPI execution backend for GraphVIPER map/reduce graphs.

This is an *optional* alternative to the Dask backend
(:func:`graphviper.graph_tools.generate_dask_workflow.generate_dask_workflow`
followed by ``dask.compute``).  The GraphVIPER architecture keeps the graph a
plain, backend-agnostic description (``map`` / optional ``reduce`` /optional
``load`` stages produced by :func:`graphviper.graph_tools.map.map` and
:func:`graphviper.graph_tools.reduce.reduce`); ``dask.delayed`` only ever appears
inside ``generate_dask_workflow``.  Because of that modularity the *same* graph
can be executed by a different engine.  This module provides an MPI engine so a
caller can swap::

    viper_graph = map(...)
    viper_graph = reduce(...)
    dask_graph  = generate_dask_workflow(viper_graph)   # Dask backend
    return_dict = dask.compute(dask_graph)[0]

for::

    viper_graph = map(...)
    viper_graph = reduce(...)
    return_dict = processes_with_mpi(viper_graph, cluster_setup)   # MPI backend

Execution model (manager / worker pool)
---------------------------------------
The backend uses :class:`mpi4py.futures.MPIPoolExecutor` in the **static**
manager-worker model.  The program must therefore be launched as::

    mpiexec -n <N> python -m mpi4py.futures <script>.py ...
    # or, on TACC:
    ibrun python -m mpi4py.futures <script>.py ...

With ``-m mpi4py.futures`` exactly one rank (rank 0) runs the user script
(``__main__``) -- it is the *manager*: it builds the graph and calls
``processes_with_mpi``.  The remaining ranks never run the script body; they sit
in a worker server loop and execute map (and, optionally, reduce) node tasks
dispatched by the manager.  This deliberately mirrors the "dedicated driver
node" pattern: the manager does the orchestration (and the small final reduce)
while every other rank does the heavy per-chunk compute -- but unlike a Dask
in-process scheduler there is no large task graph to hold, and unlike a
dedicated *Dask* scheduler node the manager rank shares its node with worker
ranks (only one rank, not a whole node, is spent on orchestration).

Pickling
--------
GraphVIPER's :func:`map` adapts an explicit-signature node task into a
``functools.wraps`` *closure* (see
:func:`graphviper.graph_tools.map.make_graph_node_task`), and plain ``pickle``
cannot serialise closures.  mpi4py is therefore reconfigured to pickle with
``cloudpickle`` on the manager side; cloudpickle's output is loadable by the
workers' stdlib ``pickle.loads`` (cloudpickle only customises *dumps*), so the
manager->worker direction (the closure node task + its ``input_params``) works
with only the manager reconfigured.

The worker->manager *result* direction is NOT symmetric: workers stay on stdlib
``pickle`` (they are already in the server loop and cannot be reconfigured from
here), so node-task **return values must be stdlib-pickle-serialisable**.  This
differs from the Dask backend (which serialises worker results with cloudpickle):
a result that is only cloudpickle-serialisable -- a closure/lambda, or an instance
of a class defined in ``__main__`` or a local scope -- works under
``dask.compute`` but raises ``PicklingError`` here.  This is fine for the imaging
use case (results are small stdlib-picklable metadata).  To make results fully
cloudpickle-capable, the launching program must call
``MPI.pickle.__init__(cloudpickle.dumps, cloudpickle.loads)`` at import time on
*every* rank (before ``mpi4py.futures`` starts the worker loop), not just inside
this function.

Notes
-----
* Map node tasks write their result chunk straight to the shared (Lustre) output
  store and return only small metadata; the manager creates/initialises that
  store *before* calling this function, so all workers see it.  Results are
  returned in input order.
* A ``load`` stage (disk-chunk coalescing) is **not** specially handled here:
  each map task self-loads its data (``input_data is None``), exactly as in the
  no-load Dask path.  Results are identical; only the cross-task I/O sharing
  optimisation is skipped.  A warning is logged if a ``load`` stage is present.
"""

import toolviper.utils.logger as logger

# Handle of the armed teardown watchdog (at most one); see force_exit_after().
_teardown_watchdog = None


def force_exit_after(seconds, note=""):
    """Arm (or re-arm) a daemon watchdog that hard-exits this process after
    ``seconds`` -- a guard against the mpi4py.futures / MPI shutdown hanging
    after all work is done.

    Rationale.  In the ``python -m mpi4py.futures`` manager-worker model the
    global stop handshake and ``MPI_Finalize`` run at *interpreter exit*.  A
    single worker rank wedged during its own shutdown (typically a non-daemon
    thread stuck in an uninterruptible filesystem syscall) blocks the
    effectively-collective finalize on every rank, idling the whole allocation
    until walltime.  An ``atexit`` hook CANNOT guard against this: CPython
    joins non-daemon threads (including ``concurrent.futures`` pools) *before*
    running atexit callbacks, which is exactly where the wedge occurs.  The
    only robust guard is a daemon thread armed while the interpreter is still
    healthy -- this function.

    Call it once the application's results are safely persisted (or use the
    ``teardown_force_exit_seconds`` option of :func:`processes_with_mpi`).
    Calling again re-arms the countdown; the previous timer is cancelled.
    ``seconds`` of ``None``/``0`` just cancels any armed watchdog.

    The forced exit uses ``os._exit(0)`` (no cleanup, no ``MPI_Finalize``); the
    MPI launcher then tears down the remaining ranks.  The launcher may log the
    exit as unclean -- harmless next to an allocation idling for hours.

    Returns the armed :class:`threading.Timer` (or ``None`` if cancelling).
    """
    import os
    import threading

    global _teardown_watchdog
    if _teardown_watchdog is not None:
        _teardown_watchdog.cancel()
        _teardown_watchdog = None
    if not seconds:
        return None

    def _fire():
        logger.warning(
            "force_exit_after: process still alive "
            f"{seconds}s after the watchdog was armed{f' ({note})' if note else ''}; "
            "forcing exit with os._exit(0) to release the allocation."
        )
        os._exit(0)

    timer = threading.Timer(seconds, _fire)
    timer.daemon = True
    timer.name = "graphviper-teardown-watchdog"
    timer.start()
    _teardown_watchdog = timer
    return timer


def _configure_cloudpickle():
    """Make the manager's mpi4py pickle node-task closures via cloudpickle.

    Returns ``True`` on success.  cloudpickle's ``dumps`` output is readable by
    stdlib ``pickle.loads`` on the worker side, so reconfiguring only the manager
    is sufficient.  No-op (returns ``False``) if cloudpickle is unavailable.
    """
    try:
        import cloudpickle
        from mpi4py import MPI
    except Exception as exc:  # pragma: no cover - depends on runtime env
        logger.warning(
            "processes_with_mpi: could not enable cloudpickle for MPI "
            f"({exc!r}); falling back to stdlib pickle. Explicit-signature node "
            "tasks (closures) will fail to serialise."
        )
        return False

    MPI.pickle.__init__(cloudpickle.dumps, cloudpickle.loads)
    return True


def _combine_tree_n_local(results, reduce_node_task, input_params, n_batch):
    """Reduce ``results`` on the manager using an ``n_batch``-ary tree.

    Pure-Python (executes immediately, no Dask).  Each layer groups the current
    results into consecutive batches of up to ``n_batch`` and replaces each
    multi-element batch with ``reduce_node_task(batch, input_params)``; a trailing
    singleton is carried forward unchanged.  ``n_batch=2`` is a binary tree.
    Because the imaging reduce is associative, this yields the same value as any
    other valid reduction order.
    """
    if n_batch < 2:
        n_batch = 2
    items = list(results)
    while len(items) > 1:
        new_items = []
        for i in range(0, len(items), n_batch):
            batch = items[i : i + n_batch]
            if len(batch) == 1:
                new_items.append(batch[0])
            else:
                new_items.append(reduce_node_task(batch, input_params))
        items = new_items
    return items[0]


def _combine_tree_n_pool(executor, results, reduce_node_task, input_params, n_batch):
    """Reduce ``results`` by submitting each reduce node to the MPI worker pool.

    Layer-synchronous: every reduce node of a layer is submitted, then all are
    awaited before the next layer.  Only worthwhile when the reduce node task is
    itself expensive; for cheap metadata merges prefer the manager-local path.
    """
    if n_batch < 2:
        n_batch = 2
    items = list(results)
    while len(items) > 1:
        pending = []  # list of (is_future, value)
        for i in range(0, len(items), n_batch):
            batch = items[i : i + n_batch]
            if len(batch) == 1:
                pending.append((False, batch[0]))
            else:
                pending.append(
                    (True, executor.submit(reduce_node_task, batch, input_params))
                )
        items = [val.result() if is_future else val for is_future, val in pending]
    return items[0]


def processes_with_mpi(viper_graph, cluster_setup=None):
    """Execute a GraphVIPER map/reduce graph with an MPI manager-worker pool.

    Drop-in replacement for ``generate_dask_workflow`` + ``dask.compute``: returns
    exactly what ``dask.compute(generate_dask_workflow(viper_graph))[0]`` would --
    the reduced result when a ``reduce`` stage is present, otherwise the list of
    per-map-task results (in input order).

    Must be launched in the static mpi4py.futures manager-worker model
    (``... python -m mpi4py.futures <script>``); see the module docstring.  Only
    the manager rank (rank 0) ever reaches this function.

    Parameters
    ----------
    viper_graph : dict
        Graph from :func:`graphviper.graph_tools.map.map` and optionally
        :func:`graphviper.graph_tools.reduce.reduce`.
    cluster_setup : dict, optional
        MPI execution options:

        * ``max_workers`` (int or None) -- cap the pool size; ``None`` (default)
          uses every available worker rank.
        * ``chunksize`` (int) -- ``MPIPoolExecutor.map`` chunk size; ``1``
          (default) gives the best dynamic load balancing for long, uneven node
          tasks (per-task dispatch overhead is negligible next to ~100 s tasks).
        * ``reduce_in_pool`` (bool) -- if ``True`` run the reduce tree on the
          worker pool; default ``False`` reduces on the manager (the right choice
          when map results are small metadata, as in imaging).
        * ``use_cloudpickle`` (bool) -- reconfigure mpi4py to pickle with
          cloudpickle so closure node tasks serialise; default ``True``.
        * ``progress_every`` (int or None) -- if set, log a progress line every
          this many completed map tasks.
        * ``teardown_force_exit_seconds`` (float or None) -- if set, arm
          :func:`force_exit_after` with this grace period when the compute
          returns, guarding against the mpi4py.futures / MPI_Finalize shutdown
          hang at interpreter exit (one wedged worker rank can idle the whole
          allocation until walltime).  Default ``None`` (off): only enable in
          run-one-graph-then-exit programs (batch jobs); a long-lived
          application that keeps working after this call would be killed
          mid-flight unless it re-arms or cancels via
          ``force_exit_after(None)``.  A subsequent ``processes_with_mpi``
          call re-arms the countdown.

    Returns
    -------
    object
        The reduced result, or (no reduce stage) the list of map-task results.
    """
    if cluster_setup is None:
        cluster_setup = {}

    max_workers = cluster_setup.get("max_workers", None)
    chunksize = cluster_setup.get("chunksize", 1)
    reduce_in_pool = cluster_setup.get("reduce_in_pool", False)
    use_cloudpickle = cluster_setup.get("use_cloudpickle", True)
    progress_every = cluster_setup.get("progress_every", None)
    teardown_force_exit_seconds = cluster_setup.get("teardown_force_exit_seconds", None)

    from mpi4py import MPI
    from mpi4py.futures import MPIPoolExecutor

    world_size = MPI.COMM_WORLD.Get_size()
    if world_size <= 1:
        logger.warning(
            "processes_with_mpi: MPI.COMM_WORLD has size 1. Launch the program "
            "with `python -m mpi4py.futures` and multiple ranks "
            "(e.g. `ibrun python -m mpi4py.futures <script>`); otherwise the "
            "MPIPoolExecutor must dynamically spawn workers, which many HPC MPIs "
            "(incl. some TACC stacks) do not support."
        )

    # The MPIPoolExecutor manager runs a background communication thread, which
    # wants MPI thread support >= SERIALIZED. Surface the provided level so a
    # thread-level mismatch (a known cause of multi-node InfiniBand hangs for
    # mpi4py.futures pools) is visible in the logs rather than a silent hang.
    try:
        thread_level = MPI.Query_thread()
        if thread_level < MPI.THREAD_SERIALIZED:
            logger.warning(
                f"processes_with_mpi: MPI thread level {thread_level} < "
                f"THREAD_SERIALIZED ({MPI.THREAD_SERIALIZED}); the MPIPoolExecutor "
                "communication thread may misbehave. If the pool hangs on a "
                "multi-node IB run, build/init mpi4py with thread support or "
                "adjust the MPI transport."
            )
        else:
            logger.info(f"processes_with_mpi: MPI thread level {thread_level}.")
    except Exception:
        pass

    if use_cloudpickle:
        _configure_cloudpickle()

    map_fn = viper_graph["map"]["node_task"]
    map_input_params = viper_graph["map"]["input_params"]
    n_tasks = len(map_input_params)

    if "load" in viper_graph:
        logger.warning(
            "processes_with_mpi: graph has a 'load' stage, but the MPI backend "
            "does not coalesce disk loads across tasks -- each map task self-loads "
            "its data (input_data=None). Results are identical; only the shared-I/O "
            "optimisation is skipped."
        )

    logger.info(
        f"processes_with_mpi: executing {n_tasks} map tasks across an MPI pool "
        f"(world size {world_size}, max_workers={max_workers}, chunksize={chunksize})."
    )

    with MPIPoolExecutor(max_workers=max_workers) as executor:
        # ---- MAP: dynamically load-balanced across the worker pool ----------
        if progress_every:
            map_results = []
            for i, res in enumerate(
                executor.map(map_fn, map_input_params, chunksize=chunksize), start=1
            ):
                map_results.append(res)
                if i % progress_every == 0 or i == n_tasks:
                    logger.info(f"processes_with_mpi: {i}/{n_tasks} map tasks done.")
        else:
            map_results = list(
                executor.map(map_fn, map_input_params, chunksize=chunksize)
            )

        # ---- REDUCE (optional) ---------------------------------------------
        if "reduce" not in viper_graph:
            result = map_results
        else:
            reduce_node_task = viper_graph["reduce"]["node_task"]
            reduce_input_params = viper_graph["reduce"]["input_params"]
            # Read mode without a default so a malformed graph fails loudly (like
            # the Dask backend, which indexes ["mode"]) instead of silently
            # tree-reducing.
            mode = viper_graph["reduce"]["mode"]
            n_batch = viper_graph["reduce"].get("n_batch", 2)

            if mode == "single_node":
                # All map outputs combined by one reduce call.
                if reduce_in_pool:
                    result = executor.submit(
                        reduce_node_task, map_results, reduce_input_params
                    ).result()
                else:
                    result = reduce_node_task(map_results, reduce_input_params)
            elif mode in ("tree", "tree_n"):
                # "tree" == tree_n with n_batch=2 (binary).
                arity = n_batch if mode == "tree_n" else 2
                if reduce_in_pool:
                    result = _combine_tree_n_pool(
                        executor,
                        map_results,
                        reduce_node_task,
                        reduce_input_params,
                        arity,
                    )
                else:
                    result = _combine_tree_n_local(
                        map_results, reduce_node_task, reduce_input_params, arity
                    )
            else:
                raise ValueError(
                    f"Unknown reduce mode {mode!r}; expected 'tree', 'tree_n', or "
                    "'single_node'."
                )

    # Outside the with-block: the executor has shut down cleanly. The remaining
    # hang risk is the global worker-stop + MPI_Finalize at interpreter exit.
    if teardown_force_exit_seconds:
        logger.info(
            "processes_with_mpi: arming teardown watchdog "
            f"(force exit in {teardown_force_exit_seconds}s if shutdown hangs)."
        )
        force_exit_after(
            teardown_force_exit_seconds, note="armed by processes_with_mpi"
        )
    return result
