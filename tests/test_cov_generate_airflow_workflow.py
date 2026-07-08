import pytest

from graphviper.graph_tools.generate_airflow_workflow import (
    generate_airflow_workflow,
    airflow_dag_to_graphviz,
)

# The whole Airflow backend is deprecated; every call emits a
# DeprecationWarning (asserted explicitly in test_airflow_backend_deprecated).
pytestmark = pytest.mark.filterwarnings("ignore:.*deprecated.*:DeprecationWarning")


# Module-level node tasks so inspect.getsource works.
def map_task(input_params):
    a = 42 + input_params["i"]
    return a


def reduce_task(map_results_list, input_params):
    return sum(map_results_list)


def _make_map_graph():
    return {
        "map": {
            "node_task": map_task,
            "input_params": [{"i": 0}, {"i": 1}, {"i": 2}],
        }
    }


def test_airflow_backend_deprecated(tmp_path):
    with pytest.deprecated_call():
        generate_airflow_workflow(_make_map_graph(), filename=str(tmp_path / "dag.py"))
    with pytest.deprecated_call():
        airflow_dag_to_graphviz(_FakeDag("d", []))


def test_generate_airflow_workflow_no_reduce(tmp_path):
    viper_graph = _make_map_graph()
    filename = str(tmp_path / "dag.py")

    generate_airflow_workflow(viper_graph, filename=filename, dag_name="my_map")

    dag_file = tmp_path / "dag.py"
    assert dag_file.exists()
    text = dag_file.read_text()
    # map task source and name present
    assert "map_task" in text
    assert "def my_map():" in text
    assert "map_task.expand(input_params=" in text
    assert "{'i': 0}" in text
    # no reduce block emitted
    assert "@task\n" not in text or "reduce_task" not in text
    assert "reduce_task" not in text


def test_generate_airflow_workflow_single_node_reduce(tmp_path):
    viper_graph = _make_map_graph()
    viper_graph["reduce"] = {
        "mode": "single_node",
        "input_params": {},
        "node_task": reduce_task,
    }
    filename = str(tmp_path / "dag.py")

    generate_airflow_workflow(viper_graph, filename=filename)

    dag_file = tmp_path / "dag.py"
    assert dag_file.exists()
    text = dag_file.read_text()
    assert "map_task" in text
    assert "reduce_task" in text
    # single_node reduce call emitted
    assert "reduce_task(map_results_list,{})" in text
    assert "def map_reduce():" in text


def test_generate_airflow_workflow_unsupported_reduce_mode(tmp_path):
    viper_graph = _make_map_graph()
    viper_graph["reduce"] = {
        "mode": "tree",
        "input_params": {},
        "node_task": reduce_task,
    }
    filename = str(tmp_path / "dag.py")

    with pytest.raises(AssertionError):
        generate_airflow_workflow(viper_graph, filename=filename)


# --- fakes for airflow_dag_to_graphviz ---
class _FakeTask:
    def __init__(self, task_id, upstream_task_ids):
        self.task_id = task_id
        self.upstream_task_ids = upstream_task_ids


class _FakeDag:
    def __init__(self, dag_id, tasks):
        self.dag_id = dag_id
        self.tasks = tasks
        self._by_id = {t.task_id: t for t in tasks}

    def get_task(self, task_id):
        return self._by_id[task_id]


def test_airflow_dag_to_graphviz():
    import graphviz

    t_a = _FakeTask("a", set())  # no upstream
    t_b = _FakeTask("b", {"a"})  # upstream edge a -> b
    dag = _FakeDag("mydag", [t_a, t_b])

    dot = airflow_dag_to_graphviz(dag)

    assert isinstance(dot, graphviz.Digraph)
    src = dot.source
    assert "Airflow DAG - mydag" in src
    assert "a" in src
    assert "b" in src
    # edge a -> b present
    assert "a -> b" in src
