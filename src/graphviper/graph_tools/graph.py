import inspect
from typing import Any, Callable, Dict, List, TypeVar, Generic

T = TypeVar('T')


class GraphVisitor(Generic[T]):
    def visit_map(self, map_node: "MapNode") -> T:
        raise NotImplementedError

    def visit_reduce(self, reduce_node: "ReduceNode") -> T:
        raise NotImplementedError

    def visit_callable(self, task: "CallableNode") -> T:
        raise NotImplementedError


class GraphNode:
    def visit(self, visitor: GraphVisitor[T]) -> T:
        raise NotImplementedError(f"Node type {self.__class__.__name__} doesn't support GraphVisitors")


class CallableNode(GraphNode):
    """
    A Callable in a computation
    """
    def __init__(self, callable: Callable[..., Any]):
        self.callable = callable

    @property
    def source(self) -> str:
        return inspect.getsource(self.callable).replace('\n','\n    ')

    @property
    def name(self) -> str:
        return self.callable.__name__

    def visit(self, visitor: GraphVisitor[T]) -> T:
        return visitor.visit_callable(self)


class MapNode(GraphNode):
    """
    A mapping step in a computation
    """
    def __init__(self, task: CallableNode, parameters: List[Dict]):
        self.task = task
        self.parameters = parameters

    def visit(self, visitor: GraphVisitor[T]) -> T:
        return visitor.visit_map(self)


class ReduceNode(GraphNode):
    def __init__(self, input: GraphNode, reducer: CallableNode, parameters: Dict, mode: {"tree", "single_node"} = "tree"):
        self.input = input
        self.reducer = reducer
        self.parameters = parameters
        self.mode = mode

    def visit(self, visitor: GraphVisitor[T]) -> T:
        return visitor.visit_reduce(self)

