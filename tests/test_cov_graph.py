"""Fast, self-contained unit tests for ``graphviper.graph_tools.graph``.

Synthetic data only: a two-node ``xr.DataTree`` plus a stand-in for XRADIO's
``xr_ps`` processing set accessor. Graphs are computed with the default
(synchronous/threaded) scheduler.
"""

import dask
import numpy as np
import pytest
import xarray as xr

from graphviper.graph_tools.graph import Graph

ANTENNAS = ["ant0", "ant1"]


class _FakeProcessingSetAccessor:
    """Minimal stand-in for XRADIO's ``xr_ps`` DataTree accessor."""

    def __init__(self, tree):
        self._tree = tree

    def get_combined_antenna_xds(self):
        return xr.Dataset(coords={"antenna_name": ANTENNAS})

    def get_combined_field_and_source_xds(self):
        return xr.Dataset(
            coords={"field_name": ["field_0"], "polarization": ["XX", "YY"]}
        )


@pytest.fixture
def processing_set(monkeypatch):
    monkeypatch.setattr(
        xr.DataTree, "xr_ps", property(_FakeProcessingSetAccessor), raising=False
    )
    return xr.DataTree.from_dict(
        {
            name: xr.Dataset(
                {"VISIBILITY": ("antenna_name", np.zeros(len(ANTENNAS)))},
                coords={"antenna_name": ANTENNAS},
            )
            for name in ("xds_0", "xds_1")
        }
    )


def _node_task(input_params):
    return input_params["task_id"] * input_params["scale"]


def _task_id(input_params):
    return input_params["task_id"]


def _reduce_sum(batch, input_params):
    return sum(batch)


def test_build_node_requires_coordinates_and_dataset():
    graph = Graph()
    with pytest.raises(ValueError, match="Coordinates must be set"):
        graph.build_node()

    graph.make_coordinates([])
    with pytest.raises(ValueError, match="Dataset must be set"):
        graph.build_node()


def test_make_coordinates(processing_set):
    graph = Graph.from_dataset(processing_set)
    graph.make_coordinates(["antenna_name", "field_name", "polarization", "unknown"])

    coords = graph._coordinates
    assert list(coords["antenna_name"]["data"]) == ANTENNAS
    assert list(coords["field_name"]["data"]) == ["field_0"]
    assert list(coords["polarization"]["data"]) == ["XX", "YY"]
    assert coords["unknown"] is None


def test_map_reduce_compute(processing_set):
    graph = Graph.from_dataset(processing_set)
    graph.make_coordinates(["antenna_name"])
    graph.build_node()

    graph.map(_node_task, parameters={"scale": 10})
    graph.reduce(_reduce_sum, parameters={}, connect="_node_task")

    # One map node per antenna (task_id 0 and 1), summed by the reduce.
    assert graph.compute() == (10,)
    assert graph._nodes["_reduce_sum"].previous is graph._nodes["_node_task"].result


def test_map_make_workflow(processing_set):
    graph = Graph.from_dataset(processing_set)
    graph.make_coordinates(["antenna_name"])
    graph.build_node()

    graph.map(_node_task, parameters={"scale": 3}, make_workflow=True)

    assert dask.compute(*graph.graph) == (0, 3)


def test_map_default_parameters(processing_set):
    graph = Graph.from_dataset(processing_set)
    graph.make_coordinates(["antenna_name"])
    graph.build_node()

    graph.map(_task_id, make_workflow=True)

    assert dask.compute(*graph.graph) == (0, 1)
    assert graph._nodes["_task_id"].parameters == {}


def test_getitem_filter_and_reset(processing_set):
    graph = Graph.from_dataset(processing_set)

    assert set(graph[["xds_0", "xds_1"]].children) == {"xds_0", "xds_1"}

    graph.filter(leaves=["xds_0"])
    assert set(graph.datatree.children) == {"xds_0"}

    graph.make_coordinates(["antenna_name"])
    graph.build_node()
    graph.map(_node_task, parameters={"scale": 1})
    graph.reset()
    assert graph._nodes == {}
    assert graph.graph is None
    assert graph._coordinates is None
    assert graph._node_mapping is None


def test_html_properties(processing_set):
    display = pytest.importorskip("IPython.display")

    graph = Graph.from_dataset(processing_set)
    graph.make_coordinates(["antenna_name"])
    graph.build_node()

    assert isinstance(graph.coordinates, display.HTML)
    assert isinstance(graph.node_mapping, display.HTML)
