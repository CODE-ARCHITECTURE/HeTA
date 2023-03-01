def dfs_task_assignment(worker_tasks, assigned_tasks, count):
    """
    worker_tasks: 工人和任务的关系，是一个字典，键是工人编号，值是该工人可完成的任务列表
    assigned_tasks: 已经被分配的任务，是一个字典，键是工人编号，值是已经被分配给该工人的任务编号
    count: 已经完成的任务数
    """
    max_count = count

    for worker, tasks in worker_tasks.items():
        # 如果这个工人还没有被分配任务
        if worker not in assigned_tasks:
            # 遍历这个工人能够完成的所有任务
            for task in tasks:
                # 如果这个任务还没有被分配给其他工人
                if task not in assigned_tasks.values():
                    # 将这个工人分配到这个任务
                    assigned_tasks[worker] = task
                    # 计算分配这个任务后能够完成的总任务数
                    new_count = count + 1
                    # 递归调用，继续分配下一个任务
                    sub_count = dfs_task_assignment(worker_tasks, assigned_tasks, new_count)
                    if sub_count > max_count:
                        max_count = sub_count
                    # 回溯，撤销分配这个任务
                    del assigned_tasks[worker]

    return max_count