from importlib.metadata import files


def delete_files(filepaths=None):
    import os
    import shutil

    if filepaths is None:
        return
    for file in filepaths:
        print(f"Removing {file}...")
        if os.path.isdir(file):
            shutil.rmtree(file)
        elif os.path.isfile(file):
            os.remove(file)


def test_map_reduce():
    from toolviper.utils.data import download
    from graphviper.graph_tools.map import map
    from graphviper.graph_tools.coordinate_utils import (
        interpolate_data_coords_onto_parallel_coords,
    )
    from graphviper.graph_tools.generate_dask_workflow import generate_dask_workflow
    import dask

    from toolviper.dask.client import local_client

    from xradio.measurement_set import (
        convert_msv2_to_processing_set,
    )

    viper_client = local_client(cores=2, memory_limit="3GB", autorestrictor=True)

    ms_name = "Antennae_North.cal.lsrk.split.ms"
    ps_store = "Antennae_North.cal.lsrk.split.ps.zarr"
    download(file=ms_name)

    convert_msv2_to_processing_set(
        in_file=ms_name,
        out_file=ps_store,
        persistence_mode="w",
        partition_scheme=["FIELD_ID"],
        parallel_mode="partition",
    )

    from xradio.measurement_set import open_processing_set

    ps = open_processing_set(
        ps_store=ps_store,
        scan_intents=["OBSERVE_TARGET#ON_SOURCE"],
    )

    # print(ps.summary())

    ms_xds = ps["Antennae_North.cal.lsrk.split_00"]

    from graphviper.graph_tools.coordinate_utils import make_parallel_coord

    parallel_coords = {}
    n_chunks = 4
    parallel_coords["baseline_id"] = make_parallel_coord(
        coord=ms_xds.baseline_id, n_chunks=n_chunks
    )

    n_chunks = 3
    parallel_coords["frequency"] = make_parallel_coord(
        coord=ms_xds.frequency, n_chunks=n_chunks
    )

    def my_func(input_params):
        from xradio.measurement_set import load_processing_set

        # print(input_params.keys())
        ps = load_processing_set(
            ps_store=input_params["input_data_store"],
            sel_parms=input_params["data_selection"],
        )
        test_sum = 0
        for ms_xds in ps.values():
            test_sum = test_sum + ms_xds.frequency[-1].data / (
                100
                * (
                    input_params["chunk_indices"][0]
                    + input_params["chunk_indices"][1]
                    + 1
                )
            )
        return test_sum  # input_params["test_input"]

    input_params = {"test_input": 42, "input_data_store": ps_store}

    node_task_data_mapping = interpolate_data_coords_onto_parallel_coords(
        parallel_coords, ps
    )

    graph = map(
        input_data=ps,
        node_task_data_mapping=node_task_data_mapping,
        node_task=my_func,
        input_params=input_params,
        in_memory_compute=False,
        client=None,
    )

    from graphviper.graph_tools import reduce
    import numpy as np

    def my_sum(graph_inputs, input_params):
        return np.sum(graph_inputs) + input_params["test_input"]

    input_params = {"test_input": 5}

    graph_reduce = reduce(
        graph, my_sum, input_params, mode="tree"
    )  # mode "tree","single_node"

    dask_graph = generate_dask_workflow(graph_reduce)

    assert dask.compute(dask_graph)[0] == 178177980857.54022
    delete_files(
        filepaths=[ms_name, ps_store],
    )


def test_ps_partition():
    import pathlib

    msv2name = "VLBA_TL016B_split.ms"
    zarrPath = "VLBA_TL016B_split.ps.zarr"

    from toolviper.utils.data import download

    download(file=msv2name)

    from xradio.measurement_set import convert_msv2_to_processing_set

    convert_msv2_to_processing_set(
        in_file=msv2name, out_file=zarrPath, partition_scheme=[], persistence_mode="w"
    )

    from xradio.measurement_set import open_processing_set

    ps = open_processing_set(zarrPath)

    # print(ps.summary())

    from graphviper.graph_tools.coordinate_utils import (
        interpolate_data_coords_onto_parallel_coords,
        make_parallel_coord,
    )

    # Let's try an empty parallel coord map first
    parallel_coords = {}
    node_task_data_mapping = interpolate_data_coords_onto_parallel_coords(
        parallel_coords=parallel_coords,
        input_data=ps,
        ps_partition=["spectral_window_name", "field_name"],
    )

    # print(node_task_data_mapping)
    assert len(node_task_data_mapping.keys()) == 4
    # We check that for each data selection the spw_id is unique:
    spw_split_success = all(
        [
            len(
                set(
                    [
                        ps[k].frequency.attrs["spectral_window_name"]
                        for k in dm["data_selection"].keys()
                    ]
                )
            )
            == 1
            for dm in node_task_data_mapping.values()
        ]
    )
    assert spw_split_success

    delete_files(
        filepaths=[msv2name, zarrPath],
    )


def test_build_load_stage():
    """Unit test for _build_load_stage: grouping and relative-selection logic."""
    from graphviper.graph_tools.map import _build_load_stage

    # 10 mapping tasks, each covering 1 frequency channel (slices 0-9).
    # Disk chunk size = 5 → two load nodes covering [0,5) and [5,10).
    input_param_list = [
        {
            "input_data_store": "fake_store",
            "data_selection": {"xds_0": {"frequency": slice(i, i + 1)}},
        }
        for i in range(10)
    ]

    load_params_list, load_node_ids, relative_sels = _build_load_stage(
        input_param_list,
        disk_chunk_sizes={"frequency": 5},
        load_node_input_params={},
    )

    # Exactly two load nodes (one per disk chunk)
    assert len(load_params_list) == 2

    # Tasks 0-4 → load node 0; tasks 5-9 → load node 1
    assert load_node_ids[:5] == [0, 0, 0, 0, 0]
    assert load_node_ids[5:] == [1, 1, 1, 1, 1]

    # Disk-level selections cover the full disk chunk
    assert load_params_list[0]["data_selection"]["xds_0"]["frequency"] == slice(0, 5)
    assert load_params_list[1]["data_selection"]["xds_0"]["frequency"] == slice(5, 10)

    # Relative selections are expressed relative to the disk chunk start
    assert relative_sels[0]["xds_0"]["frequency"] == slice(0, 1)  # abs 0→1, disk_start 0
    assert relative_sels[4]["xds_0"]["frequency"] == slice(4, 5)  # abs 4→5, disk_start 0
    assert relative_sels[5]["xds_0"]["frequency"] == slice(0, 1)  # abs 5→6, disk_start 5
    assert relative_sels[9]["xds_0"]["frequency"] == slice(4, 5)  # abs 9→10, disk_start 5


def test_build_load_stage_boundary():
    """Tasks spanning a disk chunk boundary receive load_node_id == -1."""
    from graphviper.graph_tools.map import _build_load_stage

    # Task 0: slice(3, 7) spans chunks 0 and 1 (chunk_size=5) → fallback
    # Task 1: slice(0, 3) fits entirely in chunk 0 → gets a load node
    input_param_list = [
        {
            "input_data_store": "fake_store",
            "data_selection": {"xds_0": {"frequency": slice(3, 7)}},
        },
        {
            "input_data_store": "fake_store",
            "data_selection": {"xds_0": {"frequency": slice(0, 3)}},
        },
    ]

    _, load_node_ids, relative_sels = _build_load_stage(
        input_param_list,
        disk_chunk_sizes={"frequency": 5},
        load_node_input_params={},
    )

    assert load_node_ids[0] == -1       # boundary-spanning: no load node
    assert relative_sels[0] is None
    assert load_node_ids[1] == 0        # fits in one chunk: has a load node
    assert relative_sels[1] is not None


def test_build_load_stage_extra_params():
    """Extra load_node_input_params are merged into every load node dict."""
    from graphviper.graph_tools.map import _build_load_stage

    input_param_list = [
        {
            "input_data_store": "fake_store",
            "data_selection": {"xds_0": {"frequency": slice(0, 2)}},
        }
    ]

    load_params_list, _, _ = _build_load_stage(
        input_param_list,
        disk_chunk_sizes={"frequency": 10},
        load_node_input_params={"data_group_name": "corrected"},
    )

    assert load_params_list[0]["data_group_name"] == "corrected"


def test_load_layer_generate_dask_workflow():
    """End-to-end test: load layer pre-loads disk chunks and delivers sub-selected
    data to each map task via input_params['input_data']."""
    import numpy as np
    import xarray as xr
    import dask

    from graphviper.graph_tools.map import map as viper_map
    from graphviper.graph_tools.coordinate_utils import (
        interpolate_data_coords_onto_parallel_coords,
        make_parallel_coord,
    )
    from graphviper.graph_tools.generate_dask_workflow import generate_dask_workflow

    # Synthetic dataset: vis[i] = i for frequency i in 0..11
    n_freq = 12
    freq_vals = np.arange(n_freq, dtype=float)
    xds = xr.Dataset(
        {"vis": (["frequency"], freq_vals.copy())},
        coords={
            "frequency": xr.DataArray(
                freq_vals,
                dims=["frequency"],
                attrs={"units": "Hz", "type": "spectral_coord", "velocity_frame": "lsrk"},
            )
        },
    )
    input_data = {"xds_0": xds}

    # 6 map tasks of 2 channels each; disk chunk = 4 channels → 3 load nodes
    n_map_chunks = 6
    parallel_coords = {
        "frequency": make_parallel_coord(coord=xds.frequency, n_chunks=n_map_chunks)
    }
    node_task_data_mapping = interpolate_data_coords_onto_parallel_coords(
        parallel_coords, input_data
    )

    def my_task(input_params):
        # With the load layer, input_data is pre-populated.
        ps = input_params["input_data"]
        assert ps is not None, "Expected pre-loaded data from load layer"
        return float(ps["xds_0"]["vis"].values.sum())

    def my_load_fn(load_params):
        sel = {
            k: v
            for k, v in load_params["data_selection"]["xds_0"].items()
            if v != slice(None)
        }
        ds = input_data["xds_0"].isel(sel).load()
        return {"xds_0": ds}

    graph = viper_map(
        input_data=input_data,
        node_task_data_mapping=node_task_data_mapping,
        node_task=my_task,
        input_params={"input_data_store": "synthetic"},
        in_memory_compute=False,
        data_loading_task=my_load_fn,
        disk_chunk_sizes={"frequency": 4},  # 4-channel disk chunks, 2-channel map tasks
    )

    # Verify graph structure
    assert "load" in graph
    assert len(graph["load"]["input_params"]) == 3  # 12 channels / 4 per chunk
    assert len(graph["map"]["load_node_ids"]) == n_map_chunks
    assert all(lid >= 0 for lid in graph["map"]["load_node_ids"])

    # Tasks sharing the same disk chunk should share the same load node id
    ids = graph["map"]["load_node_ids"]
    assert ids[0] == ids[1]  # tasks 0 and 1 share disk chunk 0 (freq 0-3)
    assert ids[2] == ids[3]  # tasks 2 and 3 share disk chunk 1 (freq 4-7)
    assert ids[4] == ids[5]  # tasks 4 and 5 share disk chunk 2 (freq 8-11)
    assert ids[0] != ids[2] != ids[4]

    dask_graph = generate_dask_workflow(graph)
    results = dask.compute(dask_graph)[0]

    # Each task k covers 2 consecutive channels 2k and 2k+1 → sum = 4k+1
    expected_total = sum(range(n_freq))  # 0+1+...+11 = 66
    assert abs(sum(results) - expected_total) < 1e-10


if __name__ == "__main__":
    test_map_reduce()
    test_ps_partition()
    test_build_load_stage()
    test_build_load_stage_boundary()
    test_build_load_stage_extra_params()
    test_load_layer_generate_dask_workflow()
"""
chunk_indx 0 (0, 0)
chunk_indx 1 (0, 1)
chunk_indx 2 (0, 2)
chunk_indx 3 (1, 0)
chunk_indx 4 (1, 1)
chunk_indx 5 (1, 2)
chunk_indx 6 (2, 0)
chunk_indx 7 (2, 1)
chunk_indx 8 (2, 2)
chunk_indx 9 (3, 0)
chunk_indx 10 (3, 1)
chunk_indx 11 (3, 2)

"""
