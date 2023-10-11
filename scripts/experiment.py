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
    with atomic_var.get_lock():
        atomic_var.value += 1
    print(atomic_var.value, '/', total_query_files, flush=True)

def load_matching_order(card_path, outputs_dir):
    card_set = set()
    matching_orders = {}
    with open(card_path, 'r') as file:
        for line in file.readlines():
            card_set.add(line.split(',')[0])
    for file in os.listdir(outputs_dir):
        if file not in card_set:
            continue
        with open(outputs_dir + file, 'r') as f:
            for line in f.readlines():
                if line.startswith('Spectrum Order 1 Matching order:'):
                    matching_orders[file + '.graph'] = line.split(':')[1].strip()[1:-1].replace(", ", ",")
                    break
        if file + '.graph' not in matching_orders:
            raise Exception('The query file ' + file + ' fails to find a matching order')
    return matching_orders

if __name__ == '__main__':
    data_graph = "yeast"
    home_dir = "/home/ubuntu/workspace"
    card_path = "{}/dataset/{}/query_graph.csv".format(home_dir, data_graph)
    outputs_dir = "{}/dataset/outputs_RapidMatch/{}/".format(home_dir, data_graph)
    # matching_orders <- dict('query_file_name': 'matching order', 'query_dense_16_1.graph':'0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15')
    matching_orders = load_matching_order(card_path, outputs_dir)
    parameters = {
        'data_graph_dir': '{}/dataset/{}/data_graph/'.format(home_dir, data_graph),
        'query_graph_dir' : '{}/dataset/{}/query_graph/'.format(home_dir, data_graph),
        'output_dir': '{}/SubgraphMatching/outputs/{}/'.format(home_dir, data_graph),
        'binary_path': '{}/SubgraphMatching/build/matching/SubgraphMatching.out'.format(home_dir),
        'time_limit': '7260',
        'num': 'MAX',
        'filter': 'CFL',
        'order': 'Input',
        'input_order': '',
        'engine': 'LFTJ',
        'graph_suffix': '.graph',
        'data_graph': data_graph,
        'query_graph': '',
        'output_file_name': ''
    }
    parameters_list = []
    for query_file in os.listdir(parameters['query_graph_dir']):
        if query_file not in matching_orders:
            continue
        total_query_files = total_query_files + 1
        query_graph = query_file.split('.')[0]
        cur_parameters = copy.deepcopy(parameters)
        cur_parameters['input_order'] = matching_orders[query_file]
        cur_parameters['query_graph'] = query_graph
        cur_parameters['output_file_name'] = query_graph
        parameters_list.append(cur_parameters)
    multiCoreProcessing(execute_query, parameters_list)