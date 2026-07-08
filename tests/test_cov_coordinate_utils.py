"""Fast, self-contained unit tests for graphviper.graph_tools.coordinate_utils.

No network, no dask cluster, no real measurement sets: synthetic
xarray/numpy/mocks only.
"""

import numpy as np
import pytest
import xarray as xr
import dask.array as da

from graphviper.graph_tools.coordinate_utils import (
    make_time_coord,
    make_frequency_coord,
    make_parallel_coord,
    make_parallel_coord_by_gap,
    _partition_ps_by_non_dimensions,
    interpolate_data_coords_onto_parallel_coords,
    get_disk_chunk_sizes,
)


# ---------------------------------------------------------------------------
# make_time_coord / make_frequency_coord
# ---------------------------------------------------------------------------
def test_make_time_coord():
    tc = make_time_coord(
        time_start="2020-01-01T00:00:00.000",
        time_delta=60,
        n_samples=5,
        time_scale="utc",
    )
    assert tc["dims"] == "time"
    assert len(tc["data"]) == 5
    assert tc["attrs"]["units"] == "s"
    assert tc["attrs"]["type"] == "time"
    assert tc["attrs"]["format"] == "unix"
    assert tc["attrs"]["time_scale"] == "utc"
    # Unix seconds, 60s spacing.
    diffs = np.diff(tc["data"])
    assert np.allclose(diffs, 60.0)


def test_make_frequency_coord():
    fc = make_frequency_coord(
        freq_start=1.0e9, freq_delta=1.0e8, n_channels=4, velocity_frame="lsrk"
    )
    assert fc["dims"] == "frequency"
    assert len(fc["data"]) == 4
    assert fc["data"][0] == 1.0e9
    assert np.isclose(fc["data"][1] - fc["data"][0], 1.0e8)
    assert fc["attrs"]["units"] == "Hz"
    assert fc["attrs"]["type"] == "spectral_coord"
    assert fc["attrs"]["velocity_frame"] == "lsrk"


# ---------------------------------------------------------------------------
# make_parallel_coord
# ---------------------------------------------------------------------------
def test_make_parallel_coord_dataarray_n_chunks():
    da_coord = xr.DataArray(
        np.arange(10.0),
        dims=["time"],
        attrs={"units": "s", "type": "time"},
    )
    pc = make_parallel_coord(coord=da_coord, n_chunks=2)
    assert pc["dims"] == ("time",)
    assert pc["attrs"]["units"] == "s"
    assert len(pc["data_chunks"]) == 2
    assert "data_chunks_edges" in pc
    assert "data_chunk_slices" in pc
    # First chunk edges are the min/max of the first chunk.
    assert pc["data_chunks_edges"][0] == 0.0
    # Slices reconstruct the original array.
    reconstructed = np.concatenate(
        [np.asarray(pc["data_chunks"][k]) for k in sorted(pc["data_chunks"])]
    )
    assert np.array_equal(reconstructed, np.arange(10.0))


def test_make_parallel_coord_short_array_early_break():
    # data shorter than n_chunks -> _array_split hits the `if not chunk: break`.
    coord = {"data": np.array([0.0, 1.0, 2.0]), "dims": ("x",), "attrs": {}}
    pc = make_parallel_coord(coord=coord, n_chunks=5)
    # ceil(3/5) == 1 -> 3 chunks of size 1, then break.
    assert len(pc["data_chunks"]) == 3


def test_make_parallel_coord_gap():
    data = np.array([0.0, 1.0, 2.0, 100.0, 101.0, 102.0])
    coord = {"data": data, "dims": ("time",), "attrs": {"units": "s"}}
    pc = make_parallel_coord(coord=coord, gap=10.0)
    assert len(pc["data_chunks"]) == 2
    # Split occurs at the >gap jump between 2.0 and 100.0.
    assert np.array_equal(pc["data_chunks"][0], np.array([0.0, 1.0]))
    assert np.array_equal(pc["data_chunks"][1], np.array([2.0, 100.0, 101.0, 102.0]))
    assert pc["data_chunk_slices"][0] == slice(0, 2)
    assert pc["data_chunk_slices"][1] == slice(2, 6)
    # Gap path uses the same canonical plural edges key as the n_chunks path.
    assert "data_chunks_edges" in pc
    assert "data_chunk_edges" not in pc
    assert pc["dims"] == ("time",)


def test_make_parallel_coord_gap_multidim_raises():
    coord = {"data": np.zeros((2, 3)), "dims": ("a", "b"), "attrs": {}}
    with pytest.raises(ValueError, match="one-dimensional"):
        make_parallel_coord(coord=coord, gap=1.0)


def test_make_parallel_coord_both_none_raises():
    coord = {"data": np.arange(5.0), "dims": ("x",), "attrs": {}}
    with pytest.raises(ValueError, match="Exactly one"):
        make_parallel_coord(coord=coord, n_chunks=None, gap=None)


def test_make_parallel_coord_both_given_raises():
    coord = {"data": np.arange(5.0), "dims": ("x",), "attrs": {}}
    with pytest.raises(ValueError, match="Exactly one"):
        make_parallel_coord(coord=coord, n_chunks=2, gap=1.0)


# ---------------------------------------------------------------------------
# make_parallel_coord_by_gap
# ---------------------------------------------------------------------------
def test_make_parallel_coord_by_gap_dict():
    coord = {
        "data": [0.0, 1.0, 2.0, 100.0, 101.0],
        "dims": ("t",),
        "attrs": {"units": "s"},
    }
    pc = make_parallel_coord_by_gap(coord, gap=10.0)
    assert len(pc["data_chunks"]) == 2
    assert np.array_equal(pc["data_chunks"][0], np.array([0.0, 1.0]))
    assert np.array_equal(pc["data_chunks"][1], np.array([2.0, 100.0, 101.0]))
    assert pc["dims"] == ("t",)
    assert pc["attrs"]["units"] == "s"


def test_make_parallel_coord_by_gap_dataarray():
    da_coord = xr.DataArray(
        np.array([0.0, 1.0, 50.0, 51.0]),
        dims=["t"],
        attrs={"units": "s"},
    )
    pc = make_parallel_coord_by_gap(da_coord, gap=10.0)
    assert len(pc["data_chunks"]) == 2
    assert np.array_equal(pc["data_chunks"][0], np.array([0.0]))
    assert np.array_equal(pc["data_chunks"][1], np.array([1.0, 50.0, 51.0]))


def test_make_parallel_coord_by_gap_multidim_raises():
    coord = {"data": np.zeros((2, 3)), "dims": ("a", "b"), "attrs": {}}
    with pytest.raises(ValueError):
        make_parallel_coord_by_gap(coord, gap=1.0)


# ---------------------------------------------------------------------------
# _partition_ps_by_non_dimensions
# ---------------------------------------------------------------------------
class _FakeXrMs:
    def __init__(self, info):
        self._info = info

    def get_partition_info(self):
        return self._info


class _FakeXds:
    def __init__(self, info):
        self.xr_ms = _FakeXrMs(info)


def test_partition_ps_scalar_and_list():
    ps = {
        "ds0": _FakeXds({"spectral_window_name": "spw_0", "field_name": ["f0", "f1"]}),
        "ds1": _FakeXds({"spectral_window_name": "spw_1", "field_name": ["f1"]}),
    }
    d = _partition_ps_by_non_dimensions(ps, ["spectral_window_name", "field_name"])
    # ds0 covers (spw_0,f0),(spw_0,f1); ds1 covers (spw_1,f1)
    assert d[("spw_0", "f0")] == {"ds0"}
    assert d[("spw_0", "f1")] == {"ds0"}
    assert d[("spw_1", "f1")] == {"ds1"}
    # No dataset has spw_1 & f0 together.
    assert d[("spw_1", "f0")] == set()


def test_partition_ps_scalar_unhashable_raises():
    ps = {"ds0": _FakeXds({"bad": {"unhashable": "dict"}})}
    with pytest.raises(ValueError, match="Can't split by bad"):
        _partition_ps_by_non_dimensions(ps, ["bad"])


def test_partition_ps_list_unhashable_raises():
    ps = {"ds0": _FakeXds({"bad": [{"unhashable": "dict"}]})}
    with pytest.raises(ValueError, match="Can't split by bad"):
        _partition_ps_by_non_dimensions(ps, ["bad"])


# ---------------------------------------------------------------------------
# interpolate_data_coords_onto_parallel_coords
# ---------------------------------------------------------------------------
def _numeric_dataset(coord_name, values):
    return xr.Dataset(coords={coord_name: (coord_name, np.asarray(values))})


def test_interpolate_numeric_nearest_all_boundary_branches():
    # Data spans 10..19.
    ds = _numeric_dataset("x", np.arange(10.0, 20.0))
    input_data = {"ds": ds}

    # Manually build parallel_coords WITHOUT data_chunks_edges (hits the
    # compute-when-absent branch) and WITHOUT data_chunk_slices (hits the
    # slice(None) task_coords branch). Each chunk's [first, last] value drives
    # a different boundary case:
    #   0: [0, 2]   -> both edges below range, NOT fully covering  -> slice(None) -> empty
    #   1: [5, 25]  -> both edges out, fully covers the data       -> slice(0,10)
    #   2: [8, 12]  -> low edge below (clamped to 0)               -> slice(0,3)
    #   3: [15, 22] -> high edge above (clamped to last index)     -> slice(5,10)
    #   4: [11, 13] -> both edges in range                         -> slice(1,4)
    data_chunks = {
        0: np.array([0.0, 2.0]),
        1: np.array([5.0, 25.0]),
        2: np.array([8.0, 12.0]),
        3: np.array([15.0, 22.0]),
        4: np.array([11.0, 13.0]),
    }
    parallel_coords = {
        "x": {
            "data": np.arange(30.0),
            "data_chunks": data_chunks,
            "dims": ("x",),
            "attrs": {"units": "m"},
        }
    }

    mapping = interpolate_data_coords_onto_parallel_coords(
        parallel_coords, input_data, interpolation_method="nearest"
    )
    # data_chunks_edges was computed and cached on the parallel_coord.
    assert "data_chunks_edges" in parallel_coords["x"]

    # Chunk 0: empty (both out, not covering) -> dataset removed from selection.
    assert mapping[0]["data_selection"] == {}
    # Chunk 1: fully covering -> every sample, including the last.
    assert mapping[1]["data_selection"]["ds"]["x"] == slice(0, 10)
    # Chunk 2: low edge clamped.
    assert mapping[2]["data_selection"]["ds"]["x"] == slice(0, 3)
    # Chunk 3: high edge clamped to the end of the data.
    assert mapping[3]["data_selection"]["ds"]["x"] == slice(5, 10)
    # Chunk 4: both edges inside.
    assert mapping[4]["data_selection"]["ds"]["x"] == slice(1, 4)

    # No data_chunk_slices -> task_coords slice defaults to slice(None).
    assert mapping[1]["task_coords"]["x"]["slice"] == slice(None)
    assert mapping[1]["parallel_dims"] == ["x"]


def test_interpolate_end_edge_beyond_range_keeps_last_sample():
    # Regression: a chunk whose end edge lies strictly beyond the dataset's
    # coordinate max must select through the final sample (the old behavior
    # produced slice(start, -1), silently dropping it under isel).
    ds = _numeric_dataset("x", np.arange(10.0))
    parallel_coords = {
        "x": {
            "data": np.arange(15.0),
            "data_chunks": {0: np.arange(0.0, 5.0), 1: np.arange(5.0, 15.0)},
            "data_chunks_edges": [0.0, 4.0, 5.0, 14.0],
            "dims": ("x",),
            "attrs": {"units": "m"},
        }
    }
    mapping = interpolate_data_coords_onto_parallel_coords(
        parallel_coords, {"ds": ds}, interpolation_method="nearest"
    )
    sel = mapping[1]["data_selection"]["ds"]["x"]
    assert sel == slice(5, 10)
    picked = ds.x.isel(x=sel).values
    np.testing.assert_array_equal(picked, np.arange(5.0, 10.0))


def test_interpolate_with_data_chunk_slices_present():
    # Using make_parallel_coord output: data_chunks_edges present (skip compute)
    # and data_chunk_slices present (hits the non-default task_coords slice).
    coord = {"data": np.arange(10.0), "dims": ("x",), "attrs": {"units": "m"}}
    pc = make_parallel_coord(coord=coord, n_chunks=2)
    parallel_coords = {"x": pc}
    ds = _numeric_dataset("x", np.arange(10.0))
    mapping = interpolate_data_coords_onto_parallel_coords(
        parallel_coords, {"ds": ds}, interpolation_method="nearest"
    )
    # task_coords slice comes from data_chunk_slices.
    assert mapping[0]["task_coords"]["x"]["slice"] == pc["data_chunk_slices"][0]
    assert mapping[0]["data_selection"]["ds"]["x"].start == 0


def test_interpolate_non_nearest_linear():
    ds = _numeric_dataset("x", np.arange(10.0, 20.0))
    parallel_coords = {
        "x": {
            "data": np.arange(10.0, 20.0),
            "data_chunks": {
                0: np.array([10.0, 12.0]),
                1: np.array([14.0, 16.0]),
            },
            "dims": ("x",),
            "attrs": {"units": "m"},
        }
    }
    mapping = interpolate_data_coords_onto_parallel_coords(
        parallel_coords, {"ds": ds}, interpolation_method="linear"
    )
    assert mapping[0]["data_selection"]["ds"]["x"] == slice(0, 3)
    assert mapping[1]["data_selection"]["ds"]["x"] == slice(4, 7)


def test_interpolate_string_coordinate_exact_match():
    names = np.array(["a", "b", "c", "d"], dtype="<U4")
    ds = xr.Dataset(coords={"antenna_name": ("antenna_name", names)})
    parallel_coords = {
        "antenna_name": {
            "data": names,
            "data_chunks": {
                0: np.array(["a", "b"], dtype="<U4"),
                1: np.array(["c", "z"], dtype="<U4"),  # 'z' absent -> -1
            },
            "dims": ("antenna_name",),
            "attrs": {},
        }
    }
    mapping = interpolate_data_coords_onto_parallel_coords(
        parallel_coords, {"ds": ds}, interpolation_method="nearest"
    )
    assert mapping[0]["data_selection"]["ds"]["antenna_name"] == slice(0, 2)
    # 'c' -> idx 2; 'z' is absent (sorts beyond 'd') -> clamped to the end.
    assert mapping[1]["data_selection"]["ds"]["antenna_name"] == slice(2, 4)


def test_interpolate_spw_and_frequency_raises():
    parallel_coords = {"frequency": {"data": np.arange(3.0)}}
    with pytest.raises(ValueError, match="Cannot split by both spw and frequency"):
        interpolate_data_coords_onto_parallel_coords(
            parallel_coords,
            {},
            ps_partition=["spectral_window_name"],
        )


def test_interpolate_empty_parallel_coords_with_ps_partition():
    input_data = {
        "ds0": _FakeXds({"spectral_window_name": "spw_0"}),
        "ds1": _FakeXds({"spectral_window_name": "spw_1"}),
    }
    mapping = interpolate_data_coords_onto_parallel_coords(
        {},
        input_data,
        ps_partition=["spectral_window_name"],
    )
    # One task per partition; empty parallel dims.
    assert len(mapping) == 2
    all_selected = set()
    for task in mapping.values():
        assert task["parallel_dims"] == []
        assert task["task_coords"] == {}
        all_selected |= set(task["data_selection"].keys())
    assert all_selected == {"ds0", "ds1"}


# ---------------------------------------------------------------------------
# get_disk_chunk_sizes
# ---------------------------------------------------------------------------
def _dask_var_ds(freq_chunk, n_freq=10, n_time=4):
    """Dataset shaped like an MS v4 main dataset: numpy-backed dimension
    coordinates (as xarray always loads them) and dask-backed data variables
    whose chunks carry the on-disk chunk sizes."""
    vis = da.zeros((n_time, n_freq), chunks=(n_time, freq_chunk))
    return xr.Dataset(
        data_vars={"VISIBILITY": (("time", "frequency"), vis)},
        coords={
            "time": np.arange(float(n_time)),
            "frequency": np.arange(float(n_freq)),
        },
    )


def test_get_disk_chunk_sizes_dask_backed():
    input_data = {"ds0": _dask_var_ds(4), "ds1": _dask_var_ds(2)}
    parallel_coords = {"frequency": {"data": np.arange(10.0)}}
    result = get_disk_chunk_sizes(input_data, parallel_coords)
    # Minimum first-chunk size across datasets (2 vs 4).
    assert result == {"frequency": 2}


def test_get_disk_chunk_sizes_min_across_variables():
    ds = _dask_var_ds(4)
    ds["WEIGHT"] = (("time", "frequency"), da.zeros((4, 10), chunks=(4, 3)))
    # A variable without the frequency dimension must be skipped.
    ds["TIME_CENTROID"] = (("time",), da.zeros(4, chunks=4))
    result = get_disk_chunk_sizes({"ds0": ds}, {"frequency": {}})
    # Minimum first-chunk size across data variables (3 vs 4).
    assert result == {"frequency": 3}


def test_get_disk_chunk_sizes_ignores_coordinate_chunks():
    # A dask-backed coordinate must NOT be consulted: only data variables
    # carry on-disk chunk information (dimension coordinates are eagerly
    # loaded as numpy indexes by xarray in real opens).
    ds = _dask_var_ds(4)
    ds = ds.assign_coords(
        chan_name=("frequency", da.from_array(np.arange(10.0), chunks=2))
    )
    result = get_disk_chunk_sizes({"ds0": ds}, {"frequency": {}})
    assert result == {"frequency": 4}


def test_get_disk_chunk_sizes_numpy_backed():
    ds = xr.Dataset(
        data_vars={"VISIBILITY": (("frequency",), np.zeros(10))},
        coords={"frequency": np.arange(10.0)},
    )
    result = get_disk_chunk_sizes({"ds0": ds}, {"frequency": {}})
    # No dask-backed data variable -> full axis length (one chunk).
    assert result == {"frequency": 10}


def test_get_disk_chunk_sizes_coord_only_dim():
    # Dimension present only as a coordinate (no data variable carries it):
    # falls back to the coordinate length.
    ds = xr.Dataset(coords={"frequency": np.arange(10.0)})
    result = get_disk_chunk_sizes({"ds0": ds}, {"frequency": {}})
    assert result == {"frequency": 10}


def test_get_disk_chunk_sizes_non_dimension_coord():
    # Parallel key that is a coordinate over another dimension (not itself a
    # dataset dimension): falls back to that coordinate's length.
    ds = xr.Dataset(coords={"frequency": ("row", np.arange(7.0))})
    result = get_disk_chunk_sizes({"ds0": ds}, {"frequency": {}})
    assert result == {"frequency": 7}


def test_get_disk_chunk_sizes_datatree():
    ds = _dask_var_ds(4)
    dt = xr.DataTree.from_dict({"ms0": ds, "ms1": ds})
    result = get_disk_chunk_sizes(dt, {"frequency": {}})
    assert result == {"frequency": 4}


def test_get_disk_chunk_sizes_missing_dim_warns():
    ds = _dask_var_ds(4)
    result = get_disk_chunk_sizes({"ds0": ds}, {"nonexistent_dim": {}})
    # Dimension not found in any dataset -> skipped, empty result.
    assert result == {}
