import contextlib

import dask
from collections import defaultdict


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
    return list_to_combine[0]


def _tree_combine_n(list_to_combine, reduce_node_task, input_params, n_batch):
    """Variable-arity tree reduction: combine ``n_batch`` nodes per reduce node,
    layer by layer, until a single node remains.

    Generalises :func:`_tree_combine` (which is the ``n_batch == 2`` case). Each
    layer groups the current nodes into consecutive batches of up to ``n_batch``;
    a full/partial batch of two-or-more becomes one ``dask.delayed`` reduce node,
    while a trailing batch of exactly one is carried to the next layer unchanged
    (no needless single-input reduce call). A large ``n_batch`` yields a shallow
    tree (fewer layers, more inputs combined per reduce node), trending toward the
    ``single_node`` extreme.
    """
    if n_batch < 2:
        n_batch = 2
    while len(list_to_combine) > 1:
        new_list_to_combine = []
        for i in range(0, len(list_to_combine), n_batch):
            batch = list_to_combine[i : i + n_batch]
            if len(batch) == 1:
                new_list_to_combine.append(batch[0])
            else:
                new_list_to_combine.append(
                    dask.delayed(reduce_node_task)(batch, input_params)
                )
        list_to_combine = new_list_to_combine
    return list_to_combine[0]


def _single_node(graph, reduce_node_task, input_params):
    return dask.delayed(reduce_node_task)(graph, input_params)


def _prepare_task_input(loaded_data, relative_data_selection, input_params):
    """Sub-select the task slice from a pre-loaded disk chunk and inject it into
    the task parameter dict.

    This is the single per-task node that sits between a shared load node and a
    map node.  Combining sub-selection and injection into one step avoids the
    extra intermediate dask node that would otherwise be required if they were
    separate delayed calls.

    The load node is shared across all map tasks that fall within the same
    on-disk chunk; this function is NOT shared — one instance runs per map task.

    Parameters
    ----------
    loaded_data : dict
        ``{xds_name: xarray.Dataset}`` returned by a load node covering a full
        disk chunk.
    relative_data_selection : dict
        ``{xds_name: {dim: slice}}`` with indices relative to the start of the
        pre-loaded chunk.  ``slice(None)`` entries are skipped.
    input_params : dict
        Per-task parameter dict produced by :func:`graphviper.graph_tools.map.map`.

    Returns
    -------
    dict
        Shallow copy of *input_params* with ``"input_data"`` set to the
        sub-selected dataset dict.
    """
    input_data = {}
    for xds_name, xds_isel in relative_data_selection.items():
        if xds_name not in loaded_data:
            continue
        ds = loaded_data[xds_name]
        effective_sel = {
            dim: sel for dim, sel in xds_isel.items() if sel != slice(None)
        }
        if effective_sel:
            ds = ds.isel(effective_sel)
        input_data[xds_name] = ds

    result = dict(input_params)
    result["input_data"] = input_data
    return result


def generate_dask_workflow(viper_graph):
    """Build the Dask graph (``dask.delayed`` objects) for a viper map/reduce graph.

    Parameters
    ----------
    viper_graph : dict
        The graph description produced by :func:`graphviper.graph_tools.map.map`
        (and optionally :func:`graphviper.graph_tools.reduce.reduce`).

    Returns
    -------
    list or dask.delayed
        The list of ``dask.delayed`` map nodes, or a single reduced node when a
        reduce stage is present.
    """

    dask_graph = []

    # Optional per-map-task scheduling priorities (higher runs earlier), stored
    # by graph_tools.map as a list aligned with map input_params. Dask honors
    # the ``priority`` annotation natively, so each map node just needs to be
    # created inside a dask.annotate(priority=...) context.
    task_priorities = viper_graph["map"].get("task_priorities")

    def _priority_context(i):
        if task_priorities is not None and task_priorities[i] is not None:
            return dask.annotate(priority=task_priorities[i])
        return contextlib.nullcontext()

    if "load" in viper_graph:
        # ------------------------------------------------------------------
        # Build load nodes — one dask.delayed per unique disk-chunk group.
        # Dask will compute each load node once even when multiple map nodes
        # depend on it, avoiding redundant disk reads.
        #
        # Each load node and the prepare/map tasks it feeds are annotated
        # with ``viper_load_group`` and ``viper_map_pair`` so that the
        # ViperGraphPlugin scheduler plugin can assign priorities that
        # (a) minimise the number of concurrently-loaded disk chunks and
        # (b) schedule reduction-adjacent task pairs together.
        # ------------------------------------------------------------------
        load_fn = viper_graph["load"]["node_task"]

        # A load node inherits the HIGHEST priority among its map tasks:
        # without this, unannotated load nodes keep Dask's default priority 0
        # and would outrank every negatively-prioritized map task, scheduling
        # all loads before any map runs (buffering every disk chunk at once).
        load_priorities: dict = {}
        if task_priorities is not None:
            for i, lid in enumerate(viper_graph["map"]["load_node_ids"]):
                if lid == -1 or task_priorities[i] is None:
                    continue
                load_priorities[lid] = (
                    task_priorities[i]
                    if lid not in load_priorities
                    else max(load_priorities[lid], task_priorities[i])
                )

        load_nodes = []
        for load_id, lp in enumerate(viper_graph["load"]["input_params"]):
            delayed_lp = dask.delayed(lp)
            if load_id in load_priorities:
                annotation = dask.annotate(
                    viper_load_group=load_id, priority=load_priorities[load_id]
                )
            else:
                annotation = dask.annotate(viper_load_group=load_id)
            with annotation:
                load_nodes.append(dask.delayed(load_fn)(delayed_lp))

        load_node_ids = viper_graph["map"]["load_node_ids"]
        relative_data_selections = viper_graph["map"]["relative_data_selections"]
        map_fn = viper_graph["map"]["node_task"]

        # Count tasks per load group so pair_id can be computed incrementally.
        group_task_counter: dict = defaultdict(int)

        for i, input_params in enumerate(viper_graph["map"]["input_params"]):
            load_node_id = load_node_ids[i]
            delayed_params = dask.delayed(input_params)
            if load_node_id == -1:
                # No load node for this task — fall back to per-task loading.
                with _priority_context(i):
                    dask_graph.append(dask.delayed(map_fn)(delayed_params))
            else:
                load_node = load_nodes[load_node_id]
                rel_sel = relative_data_selections[i]
                # pair_id groups consecutive tasks within the same load group
                # into the pairs that will be combined at the first level of
                # the binary tree reduction.
                pair_id = group_task_counter[load_node_id] // 2
                group_task_counter[load_node_id] += 1
                with (
                    _priority_context(i),
                    dask.annotate(
                        viper_load_group=load_node_id, viper_map_pair=pair_id
                    ),
                ):
                    task_input = dask.delayed(_prepare_task_input)(
                        load_node, rel_sel, delayed_params
                    )
                    dask_graph.append(dask.delayed(map_fn)(task_input))
    else:
        for i, input_params in enumerate(viper_graph["map"]["input_params"]):
            with _priority_context(i):
                dask_graph.append(
                    dask.delayed(viper_graph["map"]["node_task"])(
                        dask.delayed(input_params)
                    )
                )

    if "reduce" in viper_graph:
        reduce_mode = viper_graph["reduce"]["mode"]
        if reduce_mode == "tree":
            dask_graph = _tree_combine(
                dask_graph,
                viper_graph["reduce"]["node_task"],
                viper_graph["reduce"]["input_params"],
            )
        elif reduce_mode == "tree_n":
            dask_graph = _tree_combine_n(
                dask_graph,
                viper_graph["reduce"]["node_task"],
                viper_graph["reduce"]["input_params"],
                viper_graph["reduce"].get("n_batch", 2),
            )
        elif reduce_mode == "single_node":
            dask_graph = _single_node(
                dask_graph,
                viper_graph["reduce"]["node_task"],
                viper_graph["reduce"]["input_params"],
            )

    return dask_graph
