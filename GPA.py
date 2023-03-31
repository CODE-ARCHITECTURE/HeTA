from typing import List, Tuple
import networkx as nx
Task = Tuple[int, int, int, Tuple[int, int], Tuple[int, int]]
Worker = Tuple[Tuple[int, int], int, List[int]]
tasks = []
for i in range(10):
    t_a = i
    t_s = i + 1
    t_b = 10 - i
    t_o = (i, i)
    t_d = (i+1, i+1)
    task = (i, t_a, t_s, t_b, t_o, t_d)
    tasks.append(task)
workers = []
for i in range(4):
    w_l = (i, i)
    w_v = 1
    w_S = []
    worker = (w_l, w_v, w_S)
    workers.append(worker)
for task in tasks:
    for i, worker in enumerate(workers):
        if worker[0][0] <= task[4][0] and worker[0][1] <= task[4][1] and (task[2] - i) * worker[1] >= ((task[4][0] - worker[0][0])**2 + (task[4][1] - worker[0][1])**2)**0.5:
            workers[i][2].append(task[0])

for i in range(len(workers)):
    workers[i] = (workers[i][0], workers[i][1], [tasks[j][0] for j in workers[i][2]])

def decompose_dependency_graph(dependency_graph):
    def find_clique(graph, nodes, visited, start_node):
        visited.add(start_node)
        for neighbor in graph[start_node]:
            if neighbor in nodes and neighbor not in visited:
                visited = find_clique(graph, nodes, visited, neighbor)
        return visited

    def find_maximal_clique(graph, nodes):
        clique = set()
        for node in nodes:
            cur_clique = find_clique(graph, nodes, set(), node)
            if len(cur_clique) > len(clique):
                clique = cur_clique
        return clique

    cliques = []

    all_nodes = set(dependency_graph.keys())
    while all_nodes:
        clique = find_maximal_clique(dependency_graph, all_nodes)
        cliques.append(clique)
        all_nodes -= clique

    return cliques
dependency_graph = {}
for i, worker in enumerate(workers):
    for j, other_worker in enumerate(workers):
        if i != j:
            common_tasks = set(worker[2]) & set(other_worker[2])
            if common_tasks:
                if i not in dependency_graph:
                    dependency_graph[i] = set()
                if j not in dependency_graph:
                    dependency_graph[j] = set()
                dependency_graph[i].add(j)
                dependency_graph[j].add(i)
print(dependency_graph)

cliques = decompose_dependency_graph(dependency_graph)

for i, clique in enumerate(cliques):
    print(f"Independent subgraph {i+1}:")
    print("Workers:", clique)
    print("Tasks:", set().union(*[set(workers[w][2]) for w in clique]))
    print("\n")

G = nx.Graph()
for i in range(len(workers)):
    for j in range(i + 1, len(workers)):
        if set(workers[i][2]) & set(workers[j][2]):
            G.add_edge(i, j)

cliques = nx.find_cliques(G)

for i, clique in enumerate(cliques):
    print(f"Independent subgraph {i+1}:")
    print("Workers:", clique)
    print("Tasks:", set().union(*[set(workers[w][2]) for w in clique]))
    print("\n")

def balance_tree(cliques):
    cliques = list(cliques)
    if len(cliques) == 0:
        return None
    if len(cliques) == 1:
        return [cliques[0], None, None]

    mid = len(cliques) // 2

    left_subtree = balance_tree(cliques[:mid])
    right_subtree = balance_tree(cliques[mid+1:])
    return [cliques[mid], left_subtree, right_subtree]
tree = balance_tree(cliques)

for i, worker in enumerate(workers):
    task_seq = []
    curr_time = max(worker[0][0], worker[0][1])
    curr_index = -1
    while True:
        max_end_time = -1
        next_index = -1
        for j in range(len(worker[2])):
            if worker[2][j] > curr_index and tasks[worker[2][j]][2] >= curr_time:
                end_time = max(curr_time + ((tasks[worker[2][j]][4][0] - worker[0][0])**2 + (tasks[worker[2][j]][4][1] - worker[0][1])**2)**0.5 / worker[1], tasks[worker[2][j]][5][0])
                if end_time > max_end_time:
                    max_end_time = end_time
                    next_index = worker[2][j]
        if next_index == -1:
            break
        task_seq.append(next_index)
        curr_index = next_index
        curr_time = max(max_end_time, tasks[next_index][5][0])
    workers[i] = (worker[0], worker[1], task_seq)

def dfs(workers, cur_worker, cur_seq, max_seq):
    if cur_worker == len(workers):
        total_tasks = set()
        for s in cur_seq:
            total_tasks.update(s)
        max_tasks = len(total_tasks)
        if max_tasks > max_seq[0]:
            max_seq[0] = max_tasks
            max_seq[1:] = cur_seq
        return
    for t in workers[cur_worker][2]:
        if t not in cur_seq:
            task_start_time = max(workers[cur_worker][0][0], workers[cur_worker][0][1], tasks[t][1])
            task_end_time = task_start_time + ((tasks[t][4][0] - workers[cur_worker][0][0])**2 + (tasks[t][4][1] - workers[cur_worker][0][1])**2)**0.5 / workers[cur_worker][1]
            if task_end_time <= tasks[t][2]:
                new_seq = cur_seq + [(t, task_start_time, task_end_time)]
                new_loc = tasks[t][4]
                new_worker = (new_loc, workers[cur_worker][1], new_seq)
                workers[cur_worker] = new_worker
                dfs(workers, cur_worker + 1, new_seq, max_seq)
                workers[cur_worker] = (workers[cur_worker][0], workers[cur_worker][1], cur_seq)

max_seq = [0] 
dfs(workers, 0, [], max_seq)

