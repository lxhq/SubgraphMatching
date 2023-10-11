import os

def get_true_counts(outputs_dir):
    true_counts = []
    for file in os.listdir(outputs_dir):
        time, count = 0, 0
        with open(outputs_dir + file, 'r') as f:
            for line in f.readlines():
                if line.strip().startswith('Query time (seconds):'):
                    time = float(line.split(':')[1].strip())
                if line.strip().startswith('#Embeddings:'):
                    count = int(line.split(':')[1].strip())
        true_counts.append((file, count, time))
    true_counts = sorted(true_counts, key=lambda x: x[0])
    return true_counts

def save_true_counts(true_counts, true_counts_dir, file_name, time_limit):
    with open(true_counts_dir + file_name, 'w') as f:
        for file, count, time in true_counts:
            # filter out incomplete queries
            if time > time_limit:
                continue
            # filter out non exist queries
            if count == 0:
                continue
            f.write(file + ',' + str(count) + ',' + str(time) + '\n')

if __name__ == '__main__':
    # Read and save queries with true counts
    data_graph = 'yeast'
    home_dir = '/home/lxhq/Documents/workspace'
    time_limit = 0

    outputs_dir = '{}/RapidMatch/outputs/{}/'.format(home_dir, data_graph)
    true_counts_dir = '{}/RapidMatch/dataset/real_dataset/{}/'.format(home_dir, data_graph)
    true_counts = get_true_counts(outputs_dir)
    save_true_counts(true_counts, true_counts_dir, "query_graph.csv", time_limit)