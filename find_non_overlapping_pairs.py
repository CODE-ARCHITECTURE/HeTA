def find_non_overlapping_pairs_optimized(tasks):
    # 按开始时间排序任务
    sorted_tasks = sorted(tasks, key=lambda x: x['start'])
    n = len(sorted_tasks)
    non_overlapping_pairs = []
    
    # 遍历所有任务
    for i in range(n):
        task_i = sorted_tasks[i]
        end_i = task_i['start'] + task_i['duration']
        
        # 使用二分查找找到第一个开始时间 >= end_i 的任务
        left = i + 1
        right = n - 1
        first_no_overlap_index = n  # 默认值为 n，表示没有满足条件的任务
        
        while left <= right:
            mid = (left + right) // 2
            if sorted_tasks[mid]['start'] >= end_i:
                first_no_overlap_index = mid
                right = mid - 1
            else:
                left = mid + 1
        
        # 记录所有 No Overlap 的任务对
        for j in range(first_no_overlap_index, n):
            non_overlapping_pairs.append((i, j))
    
    return sorted_tasks, non_overlapping_pairs

tasks = [
    {'start': 5, 'duration': 3},
    {'start': 1, 'duration': 4},
    {'start': 3, 'duration': 2},
    {'start': 8, 'duration': 2}
]
# 执行检查
sorted_tasks, non_overlapping_pairs = find_non_overlapping_pairs_optimized(tasks)

# 输出排序后的任务和 No Overlap 的任务对
print("Sorted Tasks:")
for task in sorted_tasks:
    print(f"Start: {task['start']}, Duration: {task['duration']}")

print("\nNo Overlap Task Pairs (indices):")
for pair in non_overlapping_pairs:
    task1 = sorted_tasks[pair[0]]
    task2 = sorted_tasks[pair[1]]
    print(f"Task {pair[0]} (ends at {task1['start'] + task1['duration']}) and Task {pair[1]} (starts at {task2['start']}): No Overlap")