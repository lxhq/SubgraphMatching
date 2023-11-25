import os
import copy
from subprocess import Popen, PIPE
import multiprocessing as mp

def generate_args(binary, *params):
    arguments = [binary]
    arguments.extend(list(params))
    return arguments

def execute_binary(args):
    process = Popen(' '.join(args), shell=True, stdout=PIPE, stderr=PIPE)
    (std_output, std_error) = process.communicate()
    process.wait()
    rc = process.returncode
    return rc, std_output, std_error


def error_callback(error):
    print(f"Error info: {error}", flush=True)

def multiCoreProcessing(fn, params):
    num_cores = int(mp.cpu_count())
    print("there are " + str(num_cores) + " cores", flush=True)
    pool = mp.Pool(num_cores - 1)
    pool.map_async(fn, params, error_callback=error_callback)
    pool.close()
    pool.join()

def execute_query(parameters):
    args = generate_args(parameters['binary_path'], 
                         '-d', parameters['data_graph_dir'] + parameters['data_graph'] + parameters['graph_suffix'], 
                         '-q', parameters['query_graph_dir'] + parameters['query_graph'] + parameters['graph_suffix'], 
                        '-filter', parameters['filter'],
                        '-order', parameters['order'], 
                        '-input_order', parameters['input_order'],
                        '-engine', parameters['engine'],
                        '-time_limit', parameters['time_limit'], 
                        '-num', parameters['num'])
    (rc, std_output, std_error) = execute_binary(args)
    with open(parameters['output_dir'] + parameters['output_file_name'], 'w+') as file:
        file.write(std_output.decode())
        if std_error:
            file.write(std_error.decode())
        file.flush()

def load_valid_queries(path):
    res = set()
    with open(path, 'r') as f:
        for line in f.readlines():
            tokens = line.split(',')
            res.add(tokens[0])
    return res

if __name__ == '__main__':
    data_graph = "yeast"
    home_dir = "/home/lxhq/Documents/workspace"
    aug_name = 'aug_2'
    card_path = "{}/dataset/{}/query_graph_{}.csv".format(home_dir, data_graph, aug_name)
    parameters = {
        'data_graph_dir': '{}/dataset/{}/data_graph/'.format(home_dir, data_graph),
        'query_graph_dir' : '{}/dataset/{}/query_graph_{}/'.format(home_dir, data_graph, aug_name),
        'output_dir': '{}/SubgraphMatching/outputs/{}/{}/'.format(home_dir, data_graph, aug_name),
        'binary_path': '{}/SubgraphMatching/build/matching/SubgraphMatching.out'.format(home_dir),
        'time_limit': '7260',
        'num': 'MAX',
        'filter': 'CFL',
        'order': 'GQL',
        'input_order': '',
        'engine': 'LFTJ',
        'graph_suffix': '.graph',
        'data_graph': data_graph,
        'query_graph': '',
        'output_file_name': ''
    }
    valid_queries = load_valid_queries(card_path)
    parameters_list = []
    for query_file in os.listdir(parameters['query_graph_dir']):
        query_graph = query_file.split('.')[0]
        if query_graph not in valid_queries:
            continue
        cur_parameters = copy.deepcopy(parameters)
        cur_parameters['query_graph'] = query_graph
        cur_parameters['output_file_name'] = query_graph
        parameters_list.append(cur_parameters)
    multiCoreProcessing(execute_query, parameters_list)