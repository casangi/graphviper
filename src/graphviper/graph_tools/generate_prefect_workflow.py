def generate_prefect_workflow(viper_graph,dag_id='0',schedule_interval=None,filename='prefect_dag_test.py',dag_name='map_reduce'):
    import inspect
    map_node_task_str = inspect.getsource(viper_graph['map']['node_task']).strip().replace('\n    ', '\n')
    map_node_task_name = viper_graph['map']['node_task'].__name__
    map_input_params = str(viper_graph['map']['input_params'])

    reduce_code_str=''
    if 'reduce' in viper_graph:
        reduce_mode = viper_graph['reduce']['mode']
        reduce_input_params = str(viper_graph['reduce']['input_params'])
        reduce_node_task_name = viper_graph['reduce']['node_task'].__name__

        if reduce_mode == 'single_node':
            reduce_mode_code_str=f'{reduce_node_task_name}(map_results_list,{reduce_input_params})'
        else:
            assert False, "Unsupported reduce mode."

        reduce_node_task_str = inspect.getsource(viper_graph['reduce']['node_task']).strip().replace('\n    ', '\n')
        reduce_code_str=f'''
@task
{reduce_node_task_str}
'''

    python_code_string=f'''
from numpy import array
import graphviper.utils.logger as logger
from prefect import flow, task

@flow
def {dag_name}():
    map_results_list = {map_node_task_name}.map({map_input_params})
    {reduce_mode_code_str}

{reduce_code_str}

@task
{map_node_task_str}


{dag_name}()
'''

    prefect_dag_file = open(filename, 'w')
    prefect_dag_file.write(python_code_string)

