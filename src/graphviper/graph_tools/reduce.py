import dask


def _tree_combine(list_to_combine, chunk_function, chunk_function_input_parms):
    while len(list_to_combine) > 1:
        new_list_to_combine = []
        for i in range(0, len(list_to_combine), 2):
            if i < len(list_to_combine) - 1:
                lazy = dask.delayed(chunk_function)(
                    [list_to_combine[i], list_to_combine[i + 1]],
                    chunk_function_input_parms,
                )
            else:
                lazy = list_to_combine[i]
            new_list_to_combine.append(lazy)
        list_to_combine = new_list_to_combine
    return list_to_combine


def _single_node(graph, chunk_function, chunk_function_input_parms):
    return dask.delayed(chunk_function)(graph, chunk_function_input_parms)


def reduce(graph, chunk_function, chunk_function_input_parms, mode="tree"):
    if mode == "tree":
        graph_reduced = _tree_combine(
            graph[0], chunk_function, chunk_function_input_parms
        )
    elif mode == "single_node":
        graph_reduced = _single_node(
            graph[0], chunk_function, chunk_function_input_parms
        )

    return [graph_reduced, graph[1]]
