import os

def get_labels(outputs_dir):
    labels = {}
    for file in os.listdir(outputs_dir):
        matching_vertices = {}
        call_count = None
        embeddings = None
        with open(outputs_dir + file, 'r') as f:
            for line in f.readlines():
                if line[0].isdigit():
                    tokens = line.split(':')
                    matching_vertices[tokens[0]] = str(len(tokens[1].split(',')))
                if line.startswith('Call Count: '):
                    call_count = line.split(':')[1].strip()
                if line.startswith('#Embeddings:'):
                    embeddings = line.split(':')[1].strip()
        labels[file] = [embeddings, call_count, matching_vertices]
    return labels

def save_labels(labels, labels_path):
    with open(labels_path, 'w') as f:
        for key, values in labels.items():
            f.write(key + ';' + values[0] + ';' + values[1] + ';' + str(values[2]) + '\n')

def verify_labels(labels, cards_path):
    with open(cards_path, 'r') as file:
        for line in file.readlines():
            tokens = line.split(',')
            # if tokens[0] not in labels:
            #     raise Exception(tokens[0] + ' is not included in labels')
            # if int(tokens[1] != int(labels[tokens[0]][0])):
            #     raise Exception('The result of {} is different between label and card'.format(tokens[0]))
            if tokens[0] in labels:
                if int(tokens[1]) != int(labels[tokens[0]][0]):
                    print(tokens[1], labels[tokens[0]][0])
                    raise Exception('The result of {} is different between label and card'.format(tokens[0]))

if __name__ == '__main__':
    # Read and save queries with true counts
    data_graph = 'yeast'
    home_dir = '/home/lxhq/Documents/workspace'
    outputs_dir = '{}/SubgraphMatching/outputs/{}/'.format(home_dir, data_graph)
    labels_path = '{}/SubgraphMatching/outputs/{}_labels.csv'.format(home_dir, data_graph)
    cards_path = '{}/dataset/{}/query_graph.csv'.format(home_dir, data_graph)
    time_limit = 0
    # labels <- {'query_file_name': [card, call count, {0:1, 4:3, ...}], ...}
    labels = get_labels(outputs_dir)
    verify_labels(labels, cards_path)
    os.remove(labels_path)
    save_labels(labels, labels_path)