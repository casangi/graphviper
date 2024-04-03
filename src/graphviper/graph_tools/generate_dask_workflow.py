from typing import Callable, Any, Union, List

import dask

from graphviper.graph_tools.graph import GraphVisitor, MapNode, ReduceNode, CallableNode, GraphNode


def generate_dask_workflow(viper_graph: GraphNode):
    return viper_graph.visit(GraphToDask())


# this is an inference from the documentation about what dask.compute will accept
DaskGraph = Union[dask.delayed, List[dask.delayed], Callable[..., Any]]


class GraphToDask(GraphVisitor[DaskGraph]):
    def visit_map(self, map_node: MapNode) -> DaskGraph:
        dask_graph = []
        for input_params in map_node.parameters:
            dask_graph.append(dask.delayed(map_node.task.visit(self))(dask.delayed(input_params)))
        return dask_graph

    def visit_reduce(self, reduce_node: ReduceNode) -> DaskGraph:
        dask_graph = []
        if reduce_node.mode == "tree":
            dask_graph = self._tree_combine(reduce_node.input.visit(self), reduce_node.reducer.visit(self), reduce_node.parameters)
        elif reduce_node.mode == "single_node":
            dask_graph = dask.delayed(reduce_node.reducer.visit(self))(reduce_node.input.visit(self), reduce_node.parameters)
        return dask_graph

    @staticmethod
    def _tree_combine(list_to_combine, reduce_node_task, input_params) -> DaskGraph:
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
        return list_to_combine

    def visit_callable(self, callable_node: CallableNode) -> DaskGraph:
        return callable_node.callable
