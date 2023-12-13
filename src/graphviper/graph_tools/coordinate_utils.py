import numpy as np
import xarray as xr
from typing import Dict, Union
from xradio.vis._processing_set import processing_set
from scipy.interpolate import interp1d
import itertools

def interpolate_data_coords_onto_parallel_coords(
    parallel_coords: dict,
    input_data: Union[Dict, processing_set],
    interpolation_method: {
        "linear",
        "nearest",
        "nearest-up",
        "zero",
        "slinear",
        "quadratic",
        "cubic",
        "previous",
        "next",
    } = "nearest",
    assume_sorted: bool = True,
) -> Dict:
    """Interpolate data_coords onto parallel_coords to create the node_task_data_mapping.

    Parameters
    ----------
    parallel_coords :
        The parallel coordinates determine the parallelism of the map graph.
        The keys in the parallel coordinates can by any combination of the dimension coordinates in the input data.
        The values are XRADIO measures with an adittional key called data_chunks that devides the values in data into chunks.
        Example of parallel_coords['frequency'] with 3 chunks:
            data: [100, 200, 300, 400, 500]
            data_chunks
                0: [100, 200]
                1: [300, 400]
                2: [500]
            dims: ('frequency',)
            attrs:
                frame: 'LSRK'
                type: spectral_coord
                units: ['Hz']
    input_data :
        Can either be a processing_set or a Dictionary of xarray datasets. Only coordinates are needed so no actual data is loaded into memory.
    interpolation_method :
        The kind of interpolation method to use as described in https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.interp1d.html.
    assume_sorted :
        Are the data in parallel_coords and input_data monotonically increasing in value.
    Returns
    -------
    node_task_data_mapping :
        Nested Dict keys: task_id, xds_name, dim. Contains the slices for each dim in a xds that a node should load.
    """
    xds_data_selection = {}  # Nested Dict keys: xds_name, dim, chunk_index.
    # Loop over every dataset and interpolate onto parallel_coords.
    for xds_name in input_data:
        for dim, pc in parallel_coords.items():
            xds = input_data[xds_name]

            # Create interpolator
            interpolator = interp1d(
                input_data[xds_name][dim].values,
                np.arange(len(input_data[xds_name][dim].values)),
                kind=interpolation_method,
                bounds_error=False,
                fill_value=-1,
                # fill_value="extrapolate",
                assume_sorted=assume_sorted,
            )

            chunk_indx_start_stop = {}
            # Interpolate all the chunk edges. This is done for performance reasons.

            if "data_chunks_edges" not in pc:
                pc["data_chunks_edges"] = _array_split_edges(pc["data_chunks"])

            interp_index = interpolator(pc["data_chunks_edges"]).astype(int)
            i = 0

            #print('interp_index',interp_index)

            # Split the interp_index for each chunk and fix any boundry issues.
            for chunk_index in sorted(pc["data_chunks"].keys()):
                if interp_index[i] == -1 and interp_index[i + 1] == -1:
                    chunk_indx_start_stop[chunk_index] = slice(None)
                else:
                    if interp_index[i] == -1:
                        interp_index[i] = 0
                    if interp_index[i+1] == -1:
                        interp_index[i+1] = -2    
                    chunk_indx_start_stop[chunk_index] = slice(
                        interp_index[i], interp_index[i + 1] + 1
                    )
                i = i + 2

            xds_data_selection.setdefault(xds_name, {})[dim] = chunk_indx_start_stop

    # To create the node_task_data_mapping we now have to rearagne the order of
    iter_chunks_indices, parallel_dims = _make_iter_chunks_indices(parallel_coords)
    # print(list(iter_chunks_indices), parallel_dims)

    node_task_data_mapping = (
        {}
    )  # Nested Dict keys: task_id, [data_selection,chunk_indices,parallel_dims], xds_name, dim.

    for task_id, chunk_indices in enumerate(iter_chunks_indices):
        # print("chunk_index", task_id, chunk_indices)
        node_task_data_mapping[task_id] = {}
        node_task_data_mapping[task_id]["chunk_indices"] = chunk_indices
        node_task_data_mapping[task_id]["parallel_dims"] = parallel_dims
        node_task_data_mapping[task_id]["data_selection"] = {}

        task_coords = {}
        for i_dim, dim in enumerate(parallel_dims):
            chunk_coords = {}
            chunk_coords["data"] = parallel_coords[dim]["data_chunks"][
                chunk_indices[i_dim]
            ]
            chunk_coords["dims"] = parallel_coords[dim]["dims"]
            chunk_coords["attrs"] = parallel_coords[dim]["attrs"]
            task_coords[dim] = chunk_coords

        node_task_data_mapping[task_id]["task_coords"] = task_coords

        for xds_name in input_data.keys():
            node_task_data_mapping[task_id]["data_selection"][xds_name] = {}
            empty_chunk = False
            for i, chunk_index in enumerate(chunk_indices):
                if chunk_index in xds_data_selection[xds_name][parallel_dims[i]]:
                    node_task_data_mapping[task_id]["data_selection"][xds_name][
                        parallel_dims[i]
                    ] = xds_data_selection[xds_name][parallel_dims[i]][chunk_index]

                    if xds_data_selection[xds_name][parallel_dims[i]][
                        chunk_index
                    ] == slice(None):
                        empty_chunk = True
                else:
                    empty_chunk = True

            if (
                empty_chunk
            ):  # The xds with xds_name has no data for the parallel chunk (no slice on one of the dims).
                node_task_data_mapping[task_id]["data_selection"][xds_name] = None

    return node_task_data_mapping

def make_time_coord(
    time_start="2019-10-03T19:00:00.000",
    time_delta=3600,
    n_samples=10,
    time_scale="utc",
):
    from astropy.timeseries import TimeSeries
    from astropy.time import Time
    from astropy import units as u

    ts = np.array(
        TimeSeries(
            time_start=time_start,
            time_delta=time_delta * u.s,
            n_samples=n_samples,
        ).time.unix
    )

    return {
        "dims": "time",
        "data": ts,
        "attrs": {
            "units": "s",
            "type": "time",
            "format": "unix",
            "time_scale": time_scale,
        },
    }

def make_frequency_coord(
    freq_start=3 * 10**9,
    freq_delta=0.4 * 10**9,
    n_channels=50,
    velocity_frame="lsrk",
):
    freq_chan = (np.arange(0, n_channels) * freq_delta + freq_start).astype(float)
    return {
        "dims": "frequency",
        "data": freq_chan,
        "attrs": {
            "units": "Hz",
            "type": "spectral_coord",
            "velocity_frame": velocity_frame,
        },
    }


def make_parallel_coord(coord, n_chunks=None):
    if isinstance(coord, xr.core.dataarray.DataArray):
        coord = coord.copy(deep=True).to_dict()

    n_samples = len(coord["data"])
    parallel_coord = {}
    parallel_coord["data"] = coord["data"]

    parallel_coord["data_chunks"] = dict(
        zip(np.arange(n_chunks), _array_split(coord["data"], n_chunks))
    )

    parallel_coord["data_chunks_edges"] = _array_split_edges(
        parallel_coord["data_chunks"]
    )

    parallel_coord["dims"] = coord["dims"]
    parallel_coord["attrs"] = coord["attrs"]
    return parallel_coord


def _array_split_edges(data: Dict):
    data_chunks_edges = []
    for key in sorted(data.keys()):
        data_chunks_edges.append(data[key][0])
        data_chunks_edges.append(data[key][-1])
    return data_chunks_edges


def _array_split(data, n_chunks):
    chunk_size = int(np.ceil(len(data) / n_chunks))

    from itertools import islice

    data_iter = iter(data)

    result = []
    for _ in range(n_chunks):
        chunk = list(islice(data_iter, chunk_size))
        if not chunk:
            break
        result.append(np.array(chunk))

    return result

def _make_iter_chunks_indices(parallel_coords):
    parallel_dims = []
    list_chunk_indxs = []
    n_chunks = 1
    for dim, pc in parallel_coords.items():
        chunk_indxs = list(pc["data_chunks"].keys())
        n_chunks = n_chunks * len(chunk_indxs)
        list_chunk_indxs.append(chunk_indxs)
        parallel_dims.append(dim)

    iter_chunks_indxs = itertools.product(*list_chunk_indxs)
    return iter_chunks_indxs, parallel_dims