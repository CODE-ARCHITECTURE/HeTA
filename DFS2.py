# 定义图的邻接表
graph = {
    'A': ['B', 'C'],
    'B': ['D', 'E'],
    'C': ['F'],
    'D': [],
    'E': ['F'],
    'F': []
}

# 深度优先搜索函数
def dfs(graph, start, path, visited, all_paths):
    # 将当前节点添加到路径中
    path.append(start)
    # 标记当前节点为已访问
    visited.add(start)
    
    # 检查当前节点是否有未访问的邻居
    for neighbor in graph[start]:
        if neighbor not in visited:
            dfs(graph, neighbor, path, visited, all_paths)
    if not any(neighbor in graph for neighbor in path if neighbor not in visited):
        all_paths.append(list(path))
def dfs_simplified(graph, start, path, visited, all_paths):
    path.append(start)  # 添加当前节点到路径中
    visited.add(start)  # 标记当前节点为已访问
    
    # 检查当前节点的所有邻居
    for neighbor in graph[start]:
        if neighbor not in visited:  # 如果邻居未被访问过
            dfs_simplified(graph, neighbor, path[:], visited.copy(), all_paths)  

def dfs_final(graph, start, all_paths, path=None, visited=None):
    if path is None:
        path = []
    if visited is None:
        visited = set()
    
    path.append(start)
    visited.add(start)
    
    if not graph[start]:  # 如果当前节点没有邻居，则它是一个叶子节点（但在这个例子中，我们仍然会递归地检查，以防有环）
        all_paths.append(list(path))  # 添加路径到结果列表中（注意：这里我们复制了path，因为path将在递归调用中被修改）
    else:
        for neighbor in graph[start]:
            if neighbor not in visited:
                dfs_final(graph, neighbor, all_paths, path[:], visited.copy())  # 递归调用，注意传递path和visited的副本

def find_all_paths_from_start(graph, start):
    all_paths = []
    dfs_final(graph, start, all_paths)
    return all_paths

# 调用函数并打印结果
start_node = 'A'
all_paths = find_all_paths_from_start(graph, start_node)
for path in all_paths:
    print(path)