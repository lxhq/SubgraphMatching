import os
import networkx as nx

def load_graph(file_path):
    nodes_list = []
    edges_list = []

    with open(file_path, 'r') as file:
        for line in file.readlines():
            line = line.strip()
            tokens = line.split(' ')
            if line.startswith('v'):
                id = int(tokens[1])
                label = int(tokens[2])
                nodes_list.append((id, {"label": label}))
            elif line.startswith('e'):
                src, dst = int(tokens[1]), int(tokens[2])
                edges_list.append((src, dst))
    graph = nx.Graph()
    graph.add_nodes_from(nodes_list)
    graph.add_edges_from(edges_list)
    return graph

def get_labels(outputs_dir):
    # labels <- {'query_file_name': filtered_matching}
    # filtered_matching <- {0: [], 1: [], ...}

    labels = {}
    for file in os.listdir(outputs_dir):
        filtered_matching = {}
        vertex_num = int(file.split('_')[2])
        with open(outputs_dir + file, 'r') as f:
            for line in f.readlines():
                if line[0].isdigit():
                    tokens = line.strip().split(':')
                    filtered_matching[tokens[0]] = tokens[1].split(',')
        if len(filtered_matching) != vertex_num:
            raise Exception('Invalid output file: ' + file)
        labels[file] = filtered_matching
    return labels

def verify_subgraph_matchings(labels, cards_path):
    with open(cards_path, 'r') as file:
        for line in file.readlines():
            tokens = line.split(',')
            if tokens[0] not in labels:
                raise Exception(tokens[0] + ' is not included in labels')
            if int(tokens[1]) != int(labels[tokens[0]][0]):
                raise Exception('The result of {} is different between label and card'.format(tokens[0]))

def verify_vertex_matchings(labels, data_graph_path, query_graph_dir):
    # load in the data graph
    data_graph = load_graph(data_graph_path)
    for query_graph_name in labels.keys():
        vertex_num = int(query_graph_name.split('_')[2])
        filtered_matching = labels[query_graph_name]

        # load in the query graph
        query_graph_path = query_graph_dir + query_graph_name + '.graph'
        query_graph = load_graph(query_graph_path)

        # make sure all data graph vertices in the filtered_matching has the same label as the query graph vertex
        # make sure the degree of all data graph vertices in the filtered_matching are larger or equal to the degree of query graph vertex
        for id in range(vertex_num):
            query_vertex_label = query_graph.nodes[id]['label']
            query_vertex_degree = query_graph.degree[id]
            id = str(id)
            for vertex in filtered_matching[id]:
                vertex = int(vertex)
                if data_graph.nodes[vertex]['label'] != query_vertex_label:
                    raise Exception('Label is not consistent')
                if data_graph.degree[vertex] < query_vertex_degree:
                    raise Exception('Degree is not consistent')

def save_labels(labels, labels_path):
    with open(labels_path, 'w') as f:
        for key, value in labels.items():
            f.write(key + ';' + str(value) + '\n')

if __name__ == '__main__':
    # Read and save queries with true counts
    data_graph = 'yeast'
    home_dir = '/home/lxhq/Documents/workspace'
    aug_name = 'aug_2'
    outputs_dir = 'outputs/{}/{}/'.format(data_graph, aug_name)
    labels_path = 'outputs/{}/query_graph_{}_labels.csv'.format(data_graph, aug_name)
    data_graph_path = '{}/dataset/{}/data_graph/{}.graph'.format(home_dir, data_graph, data_graph)
    query_graph_dir = '{}/dataset/{}/query_graph_{}/'.format(home_dir, data_graph, aug_name)

    labels = get_labels(outputs_dir)
    verify_vertex_matchings(labels, data_graph_path, query_graph_dir)
    # os.remove(labels_path)
    save_labels(labels, labels_path)