def test_map_reduce():
    from graphviper.utils.data import download
    from graphviper.graph_tools.map import map
    from graphviper.graph_tools.coordinate_utils import (
        interpolate_data_coords_onto_parallel_coords,
    )
    from graphviper.graph_tools.generate_dask_workflow import generate_dask_workflow
    import dask

    from graphviper.dask.client import local_client

    viper_client = local_client(cores=2, memory_limit="3GB", autorestrictor=True)

    ps_name = "Antennae_North.cal.lsrk.split.vis.zarr"
    download(file=ps_name)

    from xradio.vis.read_processing_set import read_processing_set

    ps = read_processing_set(
        ps_name=ps_name,
        intents=["OBSERVE_TARGET#ON_SOURCE"],
        fields=None,
    )
    ms_xds = ps[
        "Antennae_North.cal.lsrk.split_ddi_0_intent_OBSERVE_TARGET#ON_SOURCE_field_id_0"
    ]

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
        from xradio.vis.load_processing_set import load_processing_set

        # print(input_params.keys())
        ps = load_processing_set(
            ps_name=input_params["input_data_store"],
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

    input_params = {}
    input_params["test_input"] = 42
    input_params["input_data_store"] = ps_name
    # print(input_params)
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

    input_params = {}
    input_params["test_input"] = 5
    graph_reduce = reduce(
        graph, my_sum, input_params, mode="tree"
    )  # mode "tree","single_node"

    dask_graph = generate_dask_workflow(graph_reduce)
    assert dask.compute(dask_graph)[0][0] == 44544495255.635056
    viper_client.close()

if __name__ == '__main__':
    test_map_reduce()


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
