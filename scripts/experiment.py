from subprocess import Popen, PIPE
import multiprocessing as mp
import copy
from multiprocessing import Value
from ctypes import c_int
from pathlib import Path
import shutil
import os

atomic_var = Value(c_int)
total_query_files = {}

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

def multi_core_processing(fn, params):
    num_cores = int(mp.cpu_count())
    print("there are " + str(num_cores) + " cores", flush=True)
    pool = mp.Pool(num_cores - 1)
    pool.map_async(fn, params, error_callback=error_callback)
    pool.close()
    pool.join()

def execute_query(parameters):
    args = generate_args(parameters['binary_path'], 
                         '-d', parameters['data_graph_path'], 
                         '-q', parameters['query_graph_path'], 
                        '-filter', parameters['filter'],
                        '-order', parameters['order'], 
                        '-engine', parameters['engine'],
                        '-num', parameters['num'])
    (rc, std_output, std_error) = execute_binary(args)
    with open(parameters['output_path'], 'w+') as file:
        file.write(std_output.decode())
        if std_error:
            file.write(std_error.decode())
        file.flush()
    with atomic_var.get_lock():
        atomic_var.value += 1
    print(atomic_var.value, '/', 
          total_query_files[parameters['data_graph_path'].split('/')[-1].split('.')[0]], 
          flush=True)

def get_valid_queries(path):
    res = set()
    with open(path, 'r') as f:
        for line in f.readlines():
            res.add(line.split(',')[0])
    return res

if __name__ == '__main__':
    data_graphs = ['yeast', 'youtube', 'wordnet']
    home_dir = '/home/lxhq/Documents/workspace'
    general_output_dir = '{}/SubgraphMatching/outputs'.format(home_dir)
    if Path(general_output_dir).is_dir():
        shutil.rmtree(general_output_dir)
    os.mkdir(general_output_dir)
    parameters = {
        'data_graph_path': '',
        'query_graph_path' : '',
        'output_path': '',
        'binary_path': '{}/SubgraphMatching/build/matching/SubgraphMatching.out'.format(home_dir),
        'num': 'MAX',
        'filter': 'CFL',
        'order': 'GQL',
        'engine': 'LFTJ',
    }

    parameters_list = []
    for data_graph in data_graphs:
        output_dir = '{}/{}'.format(general_output_dir, data_graph)
        if Path(output_dir).is_dir():
            shutil.rmtree(output_dir)
        os.mkdir(output_dir)
        valid_queries_path = '{}/dataset/{}/query_graph.csv'.format(home_dir, data_graph)
        valid_queries = get_valid_queries(valid_queries_path)
        print('There are {} queries in {}'.format(len(valid_queries), data_graph), flush=True)
        total_query_files[data_graph] = len(valid_queries)
        for query_file in valid_queries:
            cur_parameters = copy.deepcopy(parameters)
            cur_parameters['data_graph_path'] = '{}/dataset/{}/data_graph/{}.graph'.format(home_dir, data_graph, data_graph)
            cur_parameters['query_graph_path'] = '{}/dataset/{}/query_graph/{}.graph'.format(home_dir, data_graph, query_file)
            cur_parameters['output_path'] = '{}/SubgraphMatching/outputs/{}/{}.txt'.format(home_dir, data_graph, query_file)
            parameters_list.append(cur_parameters)
    multi_core_processing(execute_query, parameters_list)