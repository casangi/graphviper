def generate_airflow_workflow(viper_graph,dag_id='0',schedule_interval=None,filename='airflow_dag_test.py',dag_name='map_reduce'):

    import inspect
    map_node_task_str = inspect.getsource(viper_graph['map']['node_task']).replace('\n','\n    ')
    map_node_task_name = viper_graph['map']['node_task'].__name__
    map_input_params = str(viper_graph['map']['input_params'])

    reduce_code_str=''
    if 'reduce' in viper_graph:
        reduce_mode = viper_graph['reduce']['mode']
        reduce_input_params = str(viper_graph['reduce']['input_params'])
        reduce_node_task_name = viper_graph['reduce']['node_task'].__name__

        if reduce_mode == 'single_node':
            reduce_mode_code_str=f'''
    {reduce_node_task_name}(map_results_list,{reduce_input_params})
            '''
        else:
            assert False, "Usupported reduce mode."

        reduce_node_task_str = inspect.getsource(viper_graph['reduce']['node_task']).replace('\n','\n    ')
        reduce_code_str=f'''
    @task
    {reduce_node_task_str}
    {reduce_mode_code_str}
        '''


    #print(len(viper_graph['map']['input_params']))
    #print(viper_graph['map']['input_params'])
    #print(repr(map_node_task_str))
    #print('****')

    python_code_string=f'''
import pendulum
from airflow.decorators import dag, task

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def {dag_name}():
    import graphviper.utils.logger as logger

    @task()
    {map_node_task_str}

    from numpy import array
    map_results_list = {map_node_task_name}.expand(input_params={map_input_params})

    {reduce_code_str}
        
{dag_name}()
    '''

    airflow_dag_file = open(filename, 'w')
    airflow_dag_file.write(python_code_string)






















    #for i, node in enumerate(viper_graph['map']):
    #    map_results.append(PythonOperator(task_id=node['node_task'].__name__+'_'+str(i),python_callable=node['node_task'],op_args={'input_params':node['input_params']}))    
    #    @task()
    #    {reduce_node_task_str}







# from airflow import DAG
# from airflow.decorators import dag, task
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
# import os
# import pendulum

# def _tree_combine(list_to_combine, reduce_node_task, input_params):
#     k=0
#     while len(list_to_combine) > 1:
#         new_list_to_combine = []
#         for i in range(0, len(list_to_combine), 2):
#             if i < len(list_to_combine) - 1:
#                 lazy = PythonOperator(task_id=reduce_node_task.__name__+'_'+str(k),
#                                       python_callable=reduce_node_task,
#                                       op_args=[list_to_combine[i], list_to_combine[i + 1]])
#                 # lazy = dask.delayed(reduce_node_task)(
#                 #     [list_to_combine[i], list_to_combine[i + 1]],
#                 #     input_params,
#                 # )
#                 k = k+1
#             else:
#                 lazy = list_to_combine[i]
#             new_list_to_combine.append(lazy)
#         list_to_combine = new_list_to_combine
#     return list_to_combine


# def _single_node(graph, reduce_node_task, input_params):
#     return dask.delayed(reduce_node_task)(graph, input_params)



# def generate_airflow_workflow(viper_graph,dag_id='0',schedule_interval=None,filename='airflow_dag_test.py'):
    
#     default_args = {
#       'owner': 'airflow',
#       'start_date': pendulum.datetime(2021, 1, 1, tz="UTC"),
#     }

#     with DAG(
#       dag_id=dag_id,
#       default_args=default_args,
#       schedule_interval=schedule_interval,
#     ) as dag:
#         map_results = []
#         for i, node in enumerate(viper_graph['map']):
#             map_results.append(PythonOperator(task_id=node['node_task'].__name__+'_'+str(i),python_callable=node['node_task'],op_args={'input_params':node['input_params']}))    

#         if 'reduce' in viper_graph:
#             if viper_graph['reduce']['mode'] == "tree":
#                 reduce_graph = _tree_combine(map_results, viper_graph['reduce']['node_task'], viper_graph['reduce']['input_params'])
#             #elif viper_graph['reduce']['mode'] == "single_node":
#             #    dask_graph = _single_node(dask_graph, viper_graph['reduce']['node_task'], viper_graph['reduce']['input_params'])

#         # return dask_graph

#       # Save the DAG source code to a file
#     # with open(filename, 'w') as f:
#     #     f.write(dag.doc_md)


#         # import graphviper.utils.logger as logger
#         # # [Nodes in DAG]
#         # @task()
#         # def map_task(i):
#         #     a = 42+i
#         #     logger.info('Task i ' + str(i))
#         #     return a
        
#         # @task()
#         # def reduce_task(q):
#         #     import numpy as np
#         #     k = np.sum(np.array(q))
#         #     logger.info('1. The sum is ' + str(k))
#         #     return k

#         # # [START main_flow]
#         # result = []
#         # for i in range(5):
#         #     result.append(map_task(i))
#         # sum = reduce_task(result)
#         # # [END main_flow]
#         # logger.info('2. The sum is ' + str(sum))






#     # dask_graph = []
#     # for node in viper_graph['map']:
#     #     dask_graph.append(dask.delayed(node['node_task'])(dask.delayed(node['input_params'])))    

#     # if 'reduce' in viper_graph:
#     #     if viper_graph['reduce']['mode'] == "tree":
#     #         dask_graph = _tree_combine(dask_graph, viper_graph['reduce']['node_task'], viper_graph['reduce']['input_params'])
#     #     elif viper_graph['reduce']['mode'] == "single_node":
#     #         dask_graph = _single_node(dask_graph, viper_graph['reduce']['node_task'], viper_graph['reduce']['input_params'])

#     return dag





# Function to generate graphviz representation of the DAG
def airflow_dag_to_graphviz(dag):
    """
    Converts an Airflow DAG to a graphviz Digraph object.

    Args:
    dag: The Airflow DAG object.

    Returns:
    A graphviz.Digraph object representing the DAG.
    """
    from graphviz import Digraph
    dot = Digraph(comment=f'Airflow DAG - {dag.dag_id}')

    # Add nodes (tasks)
    for task in dag.tasks:
        dot.node(task.task_id, label=task.task_id)

    # Add edges (dependencies)
    for task in dag.tasks:
        if task.upstream_task_ids is not None:
            for upstream_task_id in task.upstream_task_ids:
                upstream_task = dag.get_task(upstream_task_id)
                dot.edge(upstream_task.task_id, task.task_id)

    return dot