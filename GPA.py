from typing import List, Tuple
import numpy
import random
import networkx as nx
from networkx.algorithms.clique import find_cliques
from networkx.algorithms.components import connected_components
from concurrent.futures import ThreadPoolExecutor
import datetime
import timeit
import time
import xlwt
import pandas as pd
import xlrd
from xlrd import xldate_as_tuple

Task = Tuple[int, int, int, Tuple[int, int], Tuple[int, int]]

Worker = Tuple[Tuple[int, int], int, List[int]]

task_data1 = xlrd.open_workbook(r'E:\experiment-elastic\\task_test1.xlsx')
task_table = task_data1.sheets()[0]
task_tables ={}

def import_excel(excel):
   i=0
   for rown in range(excel.nrows):
      array = {'sl':'','at':'','st':'','dl':'','pt':'','budget':''}
      array['sl'] = task_table.row_values(rown, start_colx=0, end_colx=2)
      array['at'] = task_table.row_values(rown, start_colx=2, end_colx=3)
      array['st'] = task_table.row_values(rown, start_colx=3, end_colx=4)
      array['dl'] = task_table.row_values(rown, start_colx=4, end_colx=6)
      array['pt'] = task_table.row_values(rown, start_colx=6, end_colx=7)
      array['budget'] = task_table.row_values(rown, start_colx=7, end_colx=8)
      task_tables[i]=array
      i+=1
if __name__ == '__main__':
   import_excel(task_table)
tasks = []
for i in range(len(task_tables)):
    t_a = task_tables[i]['at'][0]
    t_s = task_tables[i]['st'][0]
    t_b = task_tables[i]['budget'][0]
    t_o = task_tables[i]['sl']
    t_d = task_tables[i]['dl']
    task = [i, t_a, t_s, t_b, list(t_o), list(t_d)]
    tasks.append(task)

worker_data1 = xlrd.open_workbook(r'E:\experiment-elastic\\worker_test1.xlsx')
worker_table = worker_data1.sheets()[0]

worker_tables ={}

def import_excel(excel):
   i=0
   for rown in range(excel.nrows):
      array = {'location':'','speed':'','schedule':''}
      array['location'] = worker_table.row_values(rown, start_colx=0, end_colx=2)
      array['speed'] = worker_table.row_values(rown, start_colx=2, end_colx=3)
      array['schedule'] = worker_table.row_values(rown, start_colx=3, end_colx=None)
      worker_tables[i]=array
      i+=1
if __name__ == '__main__':
   import_excel(worker_table)

for i in range(len(worker_tables)):
    temp_list=[]
    for h in worker_tables[i]['schedule']:
        if h!='':
            temp_list.append(h)
    worker_tables[i]['schedule']=temp_list

workers = []
for i in range(len(worker_tables)):
    w_l = worker_tables[i]['location']
    w_v = worker_tables[i]['speed'][0]
    w_S = []
    worker = [w_l, w_v, w_S]
    workers.append(worker)

# 将任务分配给工人
for task in tasks:
    for i, worker in enumerate(workers):
        # 判断工人是否可以完成任务
        if (task[2] - task[1]) * worker[1] >= ((task[4][0] - worker[0][0])**2 + (task[4][1] - worker[0][1])**2)**0.5:
            workers[i][2].append(task[0])

for i in range(len(workers)):
    workers[i] = [workers[i][0], workers[i][1], [tasks[j][0] for j in workers[i][2]]]

def dfs_max_tasks(driver, orders, current_location, current_time, visited, result):
    max_tasks = 0
    max_task_lists = []
    task_lists = []
    tasks_from_next_order=0
    task_lists_from_next_order=[]
    for order_index in driver[2]:
        if order_index not in visited:
            order = orders[order_index]  # Adjust index to match Python's 0-based indexing
            # Calculate travel time from the current location to the order's starting location
            _, t_a, t_s, _, t_o, t_d = order
            travel_time = ((t_o[0] - current_location[0]) ** 2 + (t_o[1] - current_location[1]) ** 2) ** 0.5 / driver[1]
            travel_time2 = ((t_o[0] - t_d[0]) ** 2 + (t_o[1] - t_d[1]) ** 2) ** 0.5 / driver[1]

            # Check if the driver can arrive at the order's starting location before the processing time
            if t_s - current_time >= travel_time:
                visited.append(order_index)
                current_location = t_d
                current_time = t_s+travel_time2

                tasks_from_next_order, task_lists_from_next_order= dfs_max_tasks(driver, orders, current_location, current_time, visited, result)
                tasks_from_next_order += 1

                if tasks_from_next_order > max_tasks:
                    max_tasks = tasks_from_next_order
                    max_task_lists = [order_index] + task_lists_from_next_order
                elif tasks_from_next_order == max_tasks:
                    max_task_lists.extend([[order_index] + task_lists_from_next_order])
                # Backtrack
                visited.remove(order_index)

                number = len(visited)
                if number == 0 :
                    current_location = driver[0]
                    current_time = 0
                else :
                    current_location = orders[visited[-1]][5]
                    current_time = orders[visited[-1]][2] + ((orders[visited[-1]][4][0] - orders[visited[-1]][5][0]) ** 2 + (orders[visited[-1]][4][1] - orders[visited[-1]][5][1]) ** 2) ** 0.5 / driver[1]
    if max_tasks > 0:
        result.setdefault(max_tasks, []).extend(max_task_lists)
    return max_tasks, max_task_lists

results = {}
for i, worker in enumerate(workers):
    result = {}
    max_tasks, max_task_list = dfs_max_tasks(worker, tasks, worker[0], 0, [], result)
    results[i] = (max_tasks, max_task_list)
# 构建依赖图
G = nx.DiGraph()
for i in range(len(workers)):
    for j in range(i+1, len(workers)):
        common_tasks = set(workers[i][2]).intersection(set(workers[j][2]))
        if len(common_tasks) > 0:
            G.add_edge(i, j)

def find_all_maximal_cliques(graph):
    all_maximal_cliques = []

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(find_maximal_cliques, graph.subgraph(component)): component for component in connected_components(graph)}

        for future in futures:
            result = future.result()
            all_maximal_cliques.extend(result)

    return all_maximal_cliques

def find_maximal_cliques(graph):
    maximal_cliques = []

    while graph.nodes:
        max_clique, remaining_subgraph = maximum_clique_and_remaining(graph)

        if max_clique:
            maximal_cliques.append((max_clique, remaining_subgraph))
            graph = remaining_subgraph
        else:
            break

    return maximal_cliques

def maximum_clique_and_remaining(graph):
    cliques = list(find_cliques(graph))

    if not cliques:
        return None, graph.copy()

    max_clique = max(cliques, key=len, default=[])
    max_clique_subgraph = graph.subgraph(max_clique)

    remaining_nodes = set(graph.nodes) - set(max_clique)
    remaining_subgraph = graph.subgraph(remaining_nodes)

    return max_clique_subgraph, remaining_subgraph

Gp = nx.Graph()
Gp.add_nodes_from(G.nodes)
Gp.add_edges_from(G.edges)


all_maximal_cliques = find_all_maximal_cliques(Gp)
root_clique=[]
nodes_clique=[]
for i, (clique, remaining_subgraph) in enumerate(all_maximal_cliques):
    if i == 0: 
        root_clique.append(list(clique.nodes()))
        connected_components_list = list(connected_components(remaining_subgraph))
        for j, component in enumerate(connected_components_list):
            component_subgraph = remaining_subgraph.subgraph(component)
            component_maximal_cliques = find_maximal_cliques(component_subgraph)
            for k, (max_clique, _) in enumerate(component_maximal_cliques):
                root_clique.append(list(max_clique.nodes()))

compeleted_tasks2=[]
compeleted_tasks_number=0
temp_workers=[]
number_clique=len(root_clique)
for k in range(number_clique):
    temp_workers = []
    for l in root_clique[k]:
        temp_workers.append(workers[l])
    for i, worker in enumerate(temp_workers):
        result = {}
        for t in compeleted_tasks2:
            if t in worker[2]:
                worker[2].remove(t)
        max_tasks, max_task_list = dfs_max_tasks(worker, tasks, worker[0], 0, [], result)
        results[i] = (max_tasks, max_task_list)
        for n in range(max_tasks):
            compeleted_tasks2.append(max_task_list[n])
        compeleted_tasks_number=compeleted_tasks_number+max_tasks