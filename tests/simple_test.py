from graphviper.graph_tools.coordinate_utils import make_frequency_coord


def main():
    import dask
    from dask.distributed import Client
    import xarray as xr
    import numpy as np

    viper_client = Client(n_workers=4, threads_per_worker=1, memory_limit="4GB")

    # input("Attached dask client. Press Enter to continue with the test...")

    # This works
    def my_func():
        print("Hello from a worker!")

    delyaed_list = []
    for i in range(10):
        delyaed_list.append(dask.delayed(my_func)())
    dask.compute(delyaed_list)

    # Now using the graphviper API to build and compute a graph
    # This does not load or write any data.
    # This code hangs at the compute step.
    # The visualization of the graph looks correct, and the graph is successfully built, but the compute never finishes.
    # The dask dashboard is active but shows no activity.

    from graphviper.graph_tools.coordinate_utils import make_parallel_coord

    from graphviper.graph_tools.coordinate_utils import make_frequency_coord

    n_chunks = 3

    ps_store = "Antennae_North.cal.lsrk.split.ps.zarr"
    from xradio.measurement_set import open_processing_set

    fields = None
    ps_xdt = open_processing_set(
        ps_store="Antennae_North.cal.lsrk.split.ps.zarr",
        scan_intents=["OBSERVE_TARGET#ON_SOURCE"],
    )

    parallel_coords = {}
    coord = make_frequency_coord(
        freq_start=343928096685.9587,
        freq_delta=11231488.981445312,
        n_channels=8,
        velocity_frame="lsrk",
    )
    parallel_coords["frequency"] = make_parallel_coord(coord=coord, n_chunks=n_chunks)

    from graphviper.graph_tools.coordinate_utils import (
        interpolate_data_coords_onto_parallel_coords,
    )

    node_task_data_mapping = interpolate_data_coords_onto_parallel_coords(
        parallel_coords, ps_xdt
    )

    from graphviper.graph_tools.map import map
    from graphviper.graph_tools.generate_dask_workflow import generate_dask_workflow

    def my_func(input_params):

        print("*" * 30)
        return input_params["test_input"]

    input_params = {}
    input_params["test_input"] = 42

    viper_graph = map(
        input_data=ps_xdt,
        node_task_data_mapping=node_task_data_mapping,
        node_task=my_func,
        input_params=input_params,
    )

    dask_graph = generate_dask_workflow(viper_graph)
    dask.visualize(dask_graph, filename="map_graph")

    print("######### Just before compute ############")
    print(dask_graph)
    dask.compute(dask_graph)

    viper_client.close()


if __name__ == "__main__":
    main()
