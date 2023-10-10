from subprocess import Popen, PIPE
import multiprocessing as mp
import copy
import os
from multiprocessing import Value
from ctypes import c_int

atomic_var = Value(c_int)
total_query_files = 0

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
    # print('Start: ' + parameters['data_graph'] + ', ' + parameters['query_graph'], flush=True)
    args = generate_args(parameters['binary_path'], 
                         '-d', parameters['data_graph_dir'] + parameters['data_graph'] + parameters['graph_suffix'], 
                         '-q', parameters['query_graph_dir'] + parameters['query_graph'] + parameters['graph_suffix'], 
                        '-filter', parameters['filter'],
                        '-order', parameters['order'], 
                        '-engine', parameters['engine'],
                        '-time_limit', parameters['time_limit'], 
                        '-num', parameters['num'])
    (rc, std_output, std_error) = execute_binary(args)
    with open(parameters['output_dir'] + parameters['output_file_name'], 'w') as file:
        if std_error:
            file.write(std_error.decode())
        file.write(std_output.decode())
        file.flush()
    with atomic_var.get_lock():
        atomic_var.value += 1
    # print('Done: ' + parameters['data_graph'] + ', ' + parameters['query_graph'], flush=True)
    print(atomic_var.value, '/', total_query_files)

def load_card(card_path):
    card_dict = set()
    with open(card_path, 'r') as file:
        for line in file.readlines():
            tokens = line.split(',')
            card_dict.add(tokens[0] + '.graph')
    return card_dict

if __name__ == '__main__':
    data_graph = "yeast"
    home_dir = "/home/lxhq/Documents/workspace"
    card_path = "{}/dataset/{}/query_graph.csv".format(home_dir, data_graph)
    card_dict = load_card(card_path)
    parameters = {
        'data_graph_dir': '{}/dataset/{}/data_graph/'.format(home_dir, data_graph),
        'query_graph_dir' : '{}/dataset/{}/query_graph/'.format(home_dir, data_graph),
        'output_dir': '{}/SubgraphMatching/outputs/{}/'.format(home_dir, data_graph),
        'binary_path': '{}/SubgraphMatching/build/matching/SubgraphMatching.out'.format(home_dir),
        'time_limit': '1860',
        'num': 'MAX',
        'filter': 'CFL',
        'order': 'GQL',
        'engine': 'LFTJ',
        'graph_suffix': '.graph',
        'data_graph': data_graph,
        'query_graph': '',
        'output_file_name': ''
    }
    parameters_list = []
    for query_file in os.listdir(parameters['query_graph_dir']):
        if query_file not in card_dict:
            continue
        total_query_files = total_query_files + 1
        query_graph = query_file.split('.')[0]
        cur_parameters = copy.deepcopy(parameters)
        cur_parameters['query_graph'] = query_graph
        cur_parameters['output_file_name'] = query_graph
        parameters_list.append(cur_parameters)
    multiCoreProcessing(execute_query, parameters_list)