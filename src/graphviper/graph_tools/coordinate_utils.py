import itertools
import numbers

import numpy as np
import xarray as xr
import graphviper.utils.logger as logger

from typing import Dict, Union
from xradio.vis._processing_set import processing_set
from scipy.interpolate import interp1d


def make_time_coord(
    time_start: str = "2019-10-03T19:00:00.000",
    time_delta: numbers.Number = 3600,
    n_samples: int = 10,
    time_scale: {"tai", "tcb", "tcg", "tdb", "tt", "ut1", "utc", "local"} = "utc",
) -> Dict:
    """Convenience function that creates a time coordinate `measures dictionary <https://docs.google.com/spreadsheets/d/14a6qMap9M5r_vjpLnaBKxsR9TF4azN5LVdOxLacOX-s/edit#gid=1504318014>`_ that can be used to create :ref:`parallel_coords <parallel coords>` using :func:`make_parallel_coord` function.

    Parameters
    ----------
    time_start : str, optional
        Start time string in format YYYY-MM-DDTHH:mm:ss.SSS, by default "2019-10-03T19:00:00.000"
    time_delta : numbers.Number, optional
        The increment between time samples in seconds, by default 3600.
    n_samples : int, optional
        Number of time steps, by default 10.
    time_scale : {"tai", "tcb", "tcg", "tdb", "tt", "ut1", "utc", "local"}, optional
        `Time scale <https://docs.astropy.org/en/stable/time/#id6>`_, by default "utc".

    Returns
    -------
    Dict :
        Time coordinate `measures dictionary <https://docs.google.com/spreadsheets/d/14a6qMap9M5r_vjpLnaBKxsR9TF4azN5LVdOxLacOX-s/edit#gid=1504318014>`_.

    Notes
    -----
    Time coordinate `measures dictionary <https://docs.google.com/spreadsheets/d/14a6qMap9M5r_vjpLnaBKxsR9TF4azN5LVdOxLacOX-s/edit#gid=1504318014>`_ format::

        { "dims": "time",
            "data": time_array,
            "attrs": {
            "units": "s",
            "type": "time",
            "format": "unix",
            "time_scale": time_scale,
            },
        }

    The time_array values are in `Unix <https://docs.astropy.org/en/stable/api/astropy.time.TimeUnix.html#astropy.time.TimeUnix>`_ seconds.
    """
    from astropy.timeseries import TimeSeries
    from astropy import units as u

    time_array = np.array(
        TimeSeries(
            time_start=time_start,
            time_delta=time_delta * u.s,
            n_samples=n_samples,
        ).time.unix
    )

    return {
        "dims": "time",
        "data": time_array,
        "attrs": {
            "units": "s",
            "type": "time",
            "format": "unix",
            "time_scale": time_scale,
        },
    }


def make_frequency_coord(
    freq_start: numbers.Number = 3 * 10**9,
    freq_delta: numbers.Number = 0.4 * 10**9,
    n_channels: int = 50,
    velocity_frame: {"gcrs", "icrs", "hcrs", "lsrk", "lsrd", "lsr"} = "lsrk",
) -> Dict:
    """Convenience function that creates a frequency coordinate `measures dictionary <https://docs.google.com/spreadsheets/d/14a6qMap9M5r_vjpLnaBKxsR9TF4azN5LVdOxLacOX-s/edit#gid=1504318014>`_ that can be used to create :ref:`parallel_coords <parallel coords>` using :func:`make_parallel_coord` function.

    Parameters
    ----------
    freq_start : numbers.Number, optional
        Start frequency in Hz, by default 3 * 10**9.
    freq_delta : numbers.Number, optional
        The increment between frequency samples, by default 0.4 * 10**9.
    n_channels : int, optional
        Number of frequency steps, by default 50.
    velocity_frame : {'gcrs','icrs','hcrs','lsrk','lsrd','lsr'}, optional
        `Velocity frame <https://docs.astropy.org/en/stable/coordinates/spectralcoord.html#common-velocity-frames>`_ , by default ``lsrk``.

    Returns
    -------
    Dict :
        Frequency coordinate `measures dictionary <https://docs.google.com/spreadsheets/d/14a6qMap9M5r_vjpLnaBKxsR9TF4azN5LVdOxLacOX-s/edit#gid=1504318014>`_.

    Notes
    -----
    Frequency coordinate `measures dictionary <https://docs.google.com/spreadsheets/d/14a6qMap9M5r_vjpLnaBKxsR9TF4azN5LVdOxLacOX-s/edit#gid=1504318014>`_ format::

        { "dims": "frequency",
            "data": frequency_array,
            "attrs": {
            "units": "Hz",
            "type": "spectral_coord",
            "velocity_frame": velocity_frame,
            },
        }
    """
    frequency_array = (np.arange(0, n_channels) * freq_delta + freq_start).astype(float)
    return {
        "dims": "frequency",
        "data": frequency_array,
        "attrs": {
            "units": "Hz",
            "type": "spectral_coord",
            "velocity_frame": velocity_frame,
        },
    }


def make_parallel_coord(coord: Union[Dict, xr.DataArray], n_chunks: int) -> Dict:
    """Creates a single parallel coordinate from a `measures dictionary <https://docs.google.com/spreadsheets/d/14a6qMap9M5r_vjpLnaBKxsR9TF4azN5LVdOxLacOX-s/edit#gid=1504318014>`_ or a `xarray.DataArray <https://docs.xarray.dev/en/stable/generated/xarray.DataArray.html>`_ with `measures attributes <https://docs.google.com/spreadsheets/d/14a6qMap9M5r_vjpLnaBKxsR9TF4azN5LVdOxLacOX-s/edit#gid=1504318014>`_.

    This function only returns a single :ref:`parallel_coord <parallel coord>` to create :ref:`parallel_coords <parallel coords>` a dictionary must be created where the keys are the dimension coordinate names and the values are the respective :ref:`parallel_coord <parallel coord>`.

    Parameters
    ----------
    coord : Union[Dict, xr.DataArray]
        The input `measures dictionary <https://docs.google.com/spreadsheets/d/14a6qMap9M5r_vjpLnaBKxsR9TF4azN5LVdOxLacOX-s/edit#gid=1504318014>`_ or `xarray.DataArray <https://docs.xarray.dev/en/stable/generated/xarray.DataArray.html>`_ with `measures attributes <https://docs.google.com/spreadsheets/d/14a6qMap9M5r_vjpLnaBKxsR9TF4azN5LVdOxLacOX-s/edit#gid=1504318014>`_.
    n_chunks : int
        How many chunks to divide coord into.

    Returns
    -------
    Dict :
        Parallel coordinate dictionary. See :ref:`notes <parallel coord>` for structure of dictionary.

    Notes
    -----
    .. _parallel coord:

    Nomenclature used:
    - ``dim``: The dimension name.
    - ``n_dim_chunks``: Number of chunks into which the dimension coordinate ``dim`` has been divided.

    The structure of a parallel coordinate::

        parallel_coord = {
                'data': 1D list/np.ndarray of Number,
                'data_chunks': {
                    0 : 1D list/np.ndarray of Number,
                    ⋮
                    n_dim_chunks-1 : ...,
                }
                'data_chunk_edges': 1D list/np.ndarray of Number,
                'dims': (dim,),
                'attrs': measure attribute,
            }

    The keys with the following meanings:

    - ``data``: An array containing all the coordinate values associated with that dimension. These values do not necessarily have to match the values in the coordinates of the input data (dictionary of `xarray.Datasets <https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html>`_ or `processing_set <https://github.com/casangi/xradio/blob/main/src/xradio/vis/_processing_set.py>`_), as those are interpolated onto these values. The minimum and maximum values can be respectively larger or smaller than the values in the coordinates of individual `xarray.Datasets <https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html>`_; this will simply exclude that data from being processed. It's important to note that the :ref:`parallel_coords <parallel coords>` and the input data coordinates must have the same `measures attributes <https://docs.google.com/spreadsheets/d/14a6qMap9M5r_vjpLnaBKxsR9TF4azN5LVdOxLacOX-s/edit#gid=1504318014>`_ (reference frame, units, etc.).
    - ``data_chunks``: A dictionary where the values are chunks of the data and the keys are integers. This chunking determines the parallelism of the graph. The values in the chunks can overlap.
    - ``data_chunks_edges``: An array with the start and end values of each chunk.
    - ``dims``: The dimension coordinate name.
    - `attrs``: The `XRADIO measures attributes <https://docs.google.com/spreadsheets/d/14a6qMap9M5r_vjpLnaBKxsR9TF4azN5LVdOxLacOX-s/edit#gid=1504318014>`_ of the data.

    Parallel coordinates can be combined into a single dictionary called the :ref:`parallel_coords <parallel coords>` where the keys are the coordinate dimension names (``dim``, see :func:`interpolate_data_coords_onto_parallel_coords` notes).
    """

    if isinstance(coord, xr.core.dataarray.DataArray):
        coord = coord.copy(deep=True).to_dict(data="array") #Deep copy so that we don't accidentally modify the xr.core.dataarray.DataArray. 
       
    parallel_coord = {}
    parallel_coord["data"] = coord["data"]

    parallel_coord["data_chunks"] = _array_split(coord["data"], n_chunks)

    parallel_coord["data_chunks_edges"] = _array_split_edges(
        parallel_coord["data_chunks"]
    )

    parallel_coord["dims"] = coord["dims"]
    parallel_coord["attrs"] = coord["attrs"]
    return parallel_coord


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
    """Interpolate data_coords onto parallel_coords to create the ``node_task_data_mapping``.

    Parameters
    ----------
    parallel_coords : Dict
        The parallel coordinates determine the parallelism of the map graph.
        The keys in the parallel coordinates can by any combination of the dimension coordinates in the input data.
        See notes in docstring for structure.
    input_data : Union[Dict, processing_set]
        Can either be a `processing_set <https://github.com/casangi/xradio/blob/main/src/xradio/vis/_processing_set.py>`_ or a Dictionary of `xarray.Datasets <https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html>`_. Only coordinates are needed so no actual data is loaded into memory.
    interpolation_method :  {"linear", "nearest", "nearest-up", "zero", "slinear", "quadratic", "cubic", "previous", "next",}, optional
        The kind of interpolation method to use as described in `Scipy documentation <https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.interp1d.html>`_ , by default ``nearest``.
    assume_sorted : bool, optional
        Are the data in parallel_coords and input_data monotonically increasing in value, by default True.
    Returns
    -------
    Dict :
         Node task data mapping dictionary. See :ref:`notes <node task data mapping>` for structure of dictionary.

    Notes
    -----
    Nomenclature used:

    - ``input data``: A dictionary of `xarray.Datasets <https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html>`_ or a `processing_set <https://github.com/casangi/xradio/blob/main/src/xradio/vis/_processing_set.py>`_.
    - ``n_datasets``: The number of `xarray.Datasets <https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html>`_ in the input data.
    - ``dim_i``: The ith dimension name.
    - ``n_dims``: The number of dimensions over which parallelism will occur.
    - ``n_dim_i_chunks``: Number of chunks into which the dimension coordinate ``dim_i`` has been divided.
    - ``n_nodes``: Number of nodes in the mapping stage of a Map Reduce graph.
    - ``_{}``: If curly brackets are preceded by an underscore, it indicates a subscript and not a dictionary value.

    .. _parallel coords:
    The structure of the parallel coordinates::

        parallel_coords = {
            dim_0: {
                'data': list/np.ndarray of Number,
                'data_chunks': {
                    0 : list/np.ndarray of Number,
                    ⋮
                    n_dim_0_chunks-1 : ...,
                }
                'data_chunk_edges': list/np.ndarray of Number,
                'dims': (dim_0,),
                'attrs': measure attribute,
            }
            ⋮
            dim_(n_dims-1): ...
        }

    The ``dim_i`` dictionaries have keys with the following meanings:

    - ``data``: An array containing all the coordinate values associated with that dimension. These values do not necessarily have to match the values in the coordinates of the input data (dictionary of `xarray.Datasets <https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html>`_ or `processing_set <https://github.com/casangi/xradio/blob/main/src/xradio/vis/_processing_set.py>`_), as those are interpolated onto these values. The minimum and maximum values can be respectively larger or smaller than the values in the coordinates of individual `xarray.Datasets <https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html>`_; this will simply exclude that data from being processed. It's important to note that the :ref:`parallel_coords <parallel coords>` and the input data coordinates must have the same measured attributes (reference frame, units, etc.).
    - ``data_chunks``: A dictionary where the data is broken into chunks with integer keys. This chunking determines the parallelism of the graph. The values in the chunks can overlap.
    - ``data_chunks_edges``: An array with the start and end values of each chunk.
    - ``dims``: The dimension coordinate name.
    - `attrs``: The `XRADIO measures attributes <https://docs.google.com/spreadsheets/d/14a6qMap9M5r_vjpLnaBKxsR9TF4azN5LVdOxLacOX-s/edit#gid=1504318014>`_ of the data.

    .. _node task data mapping:
    The node_task_data_mapping is a dictionary where each key is the node id of the nodes in the mapping stage of the graph and has the following structure::

        node_task_data_mapping = {
            0 : {
                'chunk_indices': tuple of int,
                'parallel_dims': (dim_0, ..., dim_{n_dims-1}),
                'data_selection': {
                        dataset_name_0: {
                                dim_0: slice,
                                ⋮
                                dim_(n_dims-1): slice
                        }
                        ⋮
                        dataset_name_{n_dataset-1}: ...
                }
                'task_coords':
                    dim_0:{
                        'data': list/np.ndarray of Number,
                        'dims': str,
                        'attrs': measure attribute,
                    }
                    ⋮
                    dim_(n_dims-1): ...
                }
            ⋮
            n_nodes-1 : ...
        }

    Each node id dictionary has the keys with the following meaning:

    - ``chunk_indices``: The indices assigned to the data chunks in the :ref:`parallel_coords <parallel coords>`. There must be an index for each ``parallel_dims``.
    - ``parallel_dims``: The dimension coordinates over which parallelism will occur.
    - ``data_selection``: A dictionary where the keys are the names of the datasets in the `processing_set <https://github.com/casangi/xradio/blob/main/src/xradio/vis/_processing_set.py>`_, and the values are dictionaries with the coordinates and accompanying slices. If a coordinate is not included, all values will be selected.
    - ``task_coords``: The chunk of the parallel_coord that is assigned to this node.
    """
    # Nested Dict keys: xds_name, dim, chunk_index.
    xds_data_selection = {}

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

            # Split the interp_index for each chunk and fix any boundary issues.
            for chunk_index in sorted(pc["data_chunks"].keys()):
                if interp_index[i] == -1 and interp_index[i + 1] == -1:
                    chunk_indx_start_stop[chunk_index] = slice(None)
                else:
                    if interp_index[i] == -1:
                        interp_index[i] = 0
                    if interp_index[i + 1] == -1:
                        interp_index[i + 1] = -2
                    chunk_indx_start_stop[chunk_index] = slice(
                        interp_index[i], interp_index[i + 1] + 1
                    )
                i = i + 2

            xds_data_selection.setdefault(xds_name, {})[dim] = chunk_indx_start_stop

    # To create the node_task_data_mapping we have to get the slices for each dimension on node task level.
    # Using itertools we can get all the combinations of the chunk indices that belong to a given node:
    iter_chunks_indices, parallel_dims = _make_iter_chunks_indices(parallel_coords)
    node_task_data_mapping = (
        {}
    )  # Nested Dict keys: task_id, [data_selection,chunk_indices,parallel_dims], xds_name, dim.

    # Loop over every task node (each task node has a unique task_id):
    for task_id, chunk_indices in enumerate(iter_chunks_indices):
        logger.debug(f"chunk_index: {task_id}, {chunk_indices}")
        node_task_data_mapping[task_id] = {}
        node_task_data_mapping[task_id]["chunk_indices"] = chunk_indices
        node_task_data_mapping[task_id]["parallel_dims"] = parallel_dims
        node_task_data_mapping[task_id]["data_selection"] = {}

        task_coords = {}
        # For task_id get the task_coords from parallel_coords:
        for i_dim, dim in enumerate(parallel_dims):
            chunk_coords = {}
            chunk_coords["data"] = parallel_coords[dim]["data_chunks"][
                chunk_indices[i_dim]
            ]
            chunk_coords["dims"] = parallel_coords[dim]["dims"]
            chunk_coords["attrs"] = parallel_coords[dim]["attrs"]
            task_coords[dim] = chunk_coords

        node_task_data_mapping[task_id]["task_coords"] = task_coords

        # For task_id get the selection slices for each dataset in the input data from xds_data_selection:
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


def _array_split(data: Union[list, np.ndarray], n_chunks: int):
    """Takes an input array and splits it into n_chunk arrays which are stored in a dictionary with numbered keys.

    Parameters
    ----------
    data : Union[list,np.ndarray]
        Array to be split.
    n_chunks : int
        Number of chunks to split array into.

    Returns
    -------
    data_chunks
        Dictionary with array broken into chunks.
    """

    chunk_size = int(np.ceil(len(data) / n_chunks))
    from itertools import islice

    data_iter = iter(data)

    data_chunks_list = []
    for _ in range(n_chunks):
        chunk = list(islice(data_iter, chunk_size))
        if not chunk:
            break
        data_chunks_list.append(np.array(chunk))

    data_chunks = dict(zip(np.arange(n_chunks), data_chunks_list))

    return data_chunks


def _array_split_edges(data_chunks_dict: Dict):
    """
    Creates a list of the start and end values of arrays in data_chunks_dict.

    Parameters
    ----------
    data_chunks_dict : Dict
        Dictionary of integer keys and array values.

    Returns
    -------
    data_chunks_edges
        List of start and end values from data_chunks_dict.
    """
    data_chunks_edges = []
    for key in sorted(data_chunks_dict.keys()):
        data_chunks_edges.append(data_chunks_dict[key][0])
        data_chunks_edges.append(data_chunks_dict[key][-1])
    return data_chunks_edges


def _make_iter_chunks_indices(parallel_coords: Dict):
    """Creates an iterator of all the combinations of the chunks in the parallel coordinates.

    Parameters
    ----------
    parallel_coords : Dict
        See make_parallel_coord for explanation.
    Returns
    -------
    iter_chunks_indxs, parallel_dims
        The chunks of each
    """

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
