import dask

def _tree_combine(list_to_combine, reduce_node_task, input_params):
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


def _single_node(graph, reduce_node_task, input_params):
    return dask.delayed(reduce_node_task)(graph, input_params)


def generate_dask_workflow(viper_graph):

    dask_graph = []
    for input_params in viper_graph['map']['input_params']:
        dask_graph.append(dask.delayed(viper_graph['map']['node_task'])(dask.delayed(input_params)))    

    if 'reduce' in viper_graph:
        if viper_graph['reduce']['mode'] == "tree":
            dask_graph = _tree_combine(dask_graph, viper_graph['reduce']['node_task'], viper_graph['reduce']['input_params'])
        elif viper_graph['reduce']['mode'] == "single_node":
            dask_graph = _single_node(dask_graph, viper_graph['reduce']['node_task'], viper_graph['reduce']['input_params'])

    return dask_graph