# -*- coding: utf-8 -*-
"""
Created on Fri Nov 25 14:34:59 2022

@author: lz
"""

import numpy
import xlwt
import pandas as pd
import xlrd
from xlrd import xldate_as_tuple
import datetime
import timeit 
import time
import random
import csv
from copy import deepcopy
import tracemalloc

def haversine(lat1, long1, lat2, long2):
    from math import radians, sin, cos, atan2, sqrt
    lat1, long1, lat2, long2 = map(radians, [lat1, long1, lat2, long2])
    a = sin((lat1-lat2)/2)**2 + cos(lat1)*cos(lat2)*(sin((long1-long2)/2)**2)
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return 6371 * c*1000

rrr=haversine(20.0226,110.3463,20.0212,110.3249)
def listcopy(listname):
    temp=[]
    for i in listname:
        if  isinstance(i,list):
            temp.append(listcopy(i))
        else:
            temp.append(i)
    return temp

'搜索' 
def search(a, t):
    l = 0
    r = len(a) - 1
    if (t<a[l]):
        return None
    elif (t>=a[r]):
        return r
    else:
        while l <= r:
            mid = int((l+r)/2)
#            print(mid)
            if t<a[mid]:
                r = mid 
            elif (a[mid]<=t and a[mid+1]>t):
                return mid
            else:
                l = mid

'建立树结构，树节点查找'
class Node:
    def __init__(self, data):
        self.left = None
        self.right = None
        self.data = data
# Insert Node
    def insert(self, data):
#        if self.data:
            if data < self.data:
                if self.left is None:
                    self.left = Node(data)
                else:
                    self.left.insert(data)
            elif data > self.data:
                if self.right is None:
                    self.right = Node(data)
                else:
                    self.right.insert(data)
#        else:
#            self.data = data
# Print the Tree
    def PrintTree(self):
        if self.left:
            self.left.PrintTree()
        print(self.data),
        if self.right:
            self.right.PrintTree()
# 中序遍历
# Left -> Root -> Right
    def inorderTraversal(self, root):
        res = []
        if root:
            res = self.inorderTraversal(root.left)
            res.append(root.data)
            res = res + self.inorderTraversal(root.right)
        return res

'可变邻域查找法1：随机交换两个任务,最多产生max_num个解'
def find_neighbour_two(path):
    exchange = random.sample(range(1, len(path)-1), 2)
    temp_path = deepcopy(path)
    temp_path[exchange[0]] = path[exchange[1]]
    temp_path[exchange[1]] = path[exchange[0]]
    return temp_path

'可变邻域查找2：随机找两个任务放到序列最前面,产生最多max_num个邻居解'
def find_neighbour_one(path):
    # 随机选择两个city, 不改变先后顺序
    endpoints = random.sample(range(1, len(path)-1), 2)
    endpoints.sort()
    temp_path = deepcopy(path)
    temp_path[0]=path[endpoints[0]]
    temp_path[1]=path[endpoints[1]]
    temp_path[endpoints[0]]=path[0]
    temp_path[endpoints[1]]=path[1]
    return temp_path


'获得树节点，以及节点包含的信息'

def tree_built(temp_worker_tables,task_tables_1):
    tree_node=[]#统计树节点
    node_worker={}#统计树节点对应的worker
    for i in range(len(temp_worker_tables)):
        workeri_idle_period={}#记录worker i的空闲时段{idle_time:[[前一个task_number], idle time, end time,[下一个task_number]]}
        sch_task=temp_worker_tables[i]['schedule']
        if (sch_task==[]):#当worker i还没有被分配任务时
           idle_st=0
           if (idle_st not in tree_node):#若0时刻不在树节点内，则把0时刻加入树节点
               tree_node.append(idle_st)
               node_worker[idle_st]=[i]#将节点0对应的worker加入该节点内
           else:
               array1=node_worker[idle_st]
               array1.append(i)
               node_worker[idle_st]=array1
           workeri_idle_period[0]=[[],0,0+3000000000,[]]
           temp_worker_tables[i]['idle_period']=workeri_idle_period#'添加worker i的空闲信息'{idle_period:{idel time:[idle time,end time,[task_number]]}}
        else:#当worker i已被分配任务时
    #        print(i)
            worker_sch_task={}
            for j in sch_task:#获得worker i已分配的任务，并建立对应的‘任务：开始时间’字典
                worker_sch_task[j]=task_tables_1[j]['st']#{task_1:st_1,task_2:st_2}
            order_worker_sch_task=sorted(worker_sch_task.items(), key = lambda kv:(kv[1], kv[0]))#对新建立的字典排序
            workeri_tree_node=[]#求出worker i包含的树节点                
            if (len(order_worker_sch_task)==1):
                task_number=order_worker_sch_task[0][0]
                idle_time=task_tables_1[task_number]['st'][0]+task_tables_1[task_number]['pt'][0]
                if (order_worker_sch_task[0][1][0]==0):
                    workeri_idle_period[idle_time]=[[task_number],idle_time,idle_time+3000000000,[]]
                else:
                    workeri_idle_period[0]=[[],0,task_tables_1[task_number]['st'][0],[task_number]]
                    workeri_idle_period[idle_time]=[[task_number],idle_time,idle_time+3000000000,[]]
            else:
                if (order_worker_sch_task[0][1][0]!=0):
                    workeri_idle_period[0]=[[],0,task_tables_1[order_worker_sch_task[0][0]]['st'][0],[order_worker_sch_task[0][0]]]
                for k in range(len(order_worker_sch_task)-1):
                    task_number=order_worker_sch_task[k][0]
                    idle_time=task_tables_1[task_number]['st'][0]+task_tables_1[task_number]['pt'][0]
                    workeri_idle_period[idle_time]=[[task_number],idle_time,task_tables_1[order_worker_sch_task[k+1][0]]['st'][0],[order_worker_sch_task[k+1][0]]]
                end_task_number=order_worker_sch_task[len(order_worker_sch_task)-1][0]
                end_task_idle_time=task_tables_1[end_task_number]['st'][0]+task_tables_1[end_task_number]['pt'][0]
                workeri_idle_period[end_task_idle_time]=[[end_task_number],end_task_idle_time,end_task_idle_time+3000000000,[]]
            temp_worker_tables[i]['idle_period']=workeri_idle_period
            workeri_tree_node=list(temp_worker_tables[i]['idle_period'].keys())#统计worker i的空闲开始时间
            '''
            for k in range(len(order_worker_sch_task)):
                task_number=order_worker_sch_task[k][0]
                idle_time=task_tables_1[task_number]['st'][0]+task_tables_1[task_number]['pt'][0]
                workeri_tree_node.append(idle_time)
            if (order_worker_sch_task[0][1][0]!=0):
                workeri_tree_node.append(0)
            '''    
            for t in workeri_tree_node:#把worker i对应的idle开始时间，加入树节点tree_node，并将worker加入对应节点node_worker内
               if (t not in tree_node):
                   tree_node.append(t)#若t时刻不在树节点内，则把t时刻加入树节点
                   node_worker[t]=[i]#将worker i加入对应节点node_worker内 
               else:               
                   array1=node_worker[t]
                   array1.append(i)
                   node_worker[t]=array1
    return (tree_node,node_worker,temp_worker_tables)

'读取固有worker的信息'
worker_data1 = xlrd.open_workbook(r'E:\experiment-elastic\\worker_test.xlsx')
worker_table = worker_data1.sheets()[0]
#创建一个空列表，存储Excel的数据
worker_tables ={}
#将excel表格内容导入到tables列表中
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
   #将excel表格的内容导入到列表中
   import_excel(worker_table)

'剔除schedule中的空集'
for i in range(len(worker_tables)):
    temp_list=[]
    for h in worker_tables[i]['schedule']:
        if h!='':
            temp_list.append(h)
    worker_tables[i]['schedule']=temp_list

'读取task的信息'
task_data1 = xlrd.open_workbook(r'E:\experiment-elastic\\task_test.xlsx')
task_table = task_data1.sheets()[0]
#创建一个空列表，存储Excel的数据
task_tables ={}
#将excel表格内容导入到tables列表中
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
   #将excel表格的内容导入到列表中
   import_excel(task_table)

template_task_tables=deepcopy(task_tables)
optimal_result=0
optimal_task_tables=deepcopy(template_task_tables)
x=0
constant=1
constant2=1
time_limit=0
'时间测试'
dateTime_s=time.time() #获取当前时间戳
dateTime_s=datetime.datetime.fromtimestamp(dateTime_s) #将时间戳转换为日期
print('dateTime_s=',dateTime_s)
tracemalloc.start()
total_utility=0
x+=1    
template_worker_tables=deepcopy(worker_tables)

'获得树节点，以及节点包含的信息'
(tree_node,node_worker,template_worker_tables)=tree_built(template_worker_tables,template_task_tables)#固有worker的
print("tree_node=",tree_node[0])
print("template_worker_tables=",template_worker_tables[0])
tree_node2=[]
node_worker2={}
worker_tables2={}
root=Node(0)
flag=0
'处理每个任务'   
waiting_task=[]#记录未能被分配的worker
'第一步：对固有worker建立树结构，树节点查找，节点内worker选择'
root3=Node(tree_node[0])#根节点需要为非0
for i in range(len(tree_node)):
    root3.insert(tree_node[i])
array3=root3.inorderTraversal(root3) 

'将一半的task作为已经分配的'
task_number1=len(template_task_tables)#int(len(template_task_tables)/2)
'时间测试'
dateTime_greedy_s=datetime.datetime.now() #获取当前时间戳
print('dateTime_greedy_s1=',dateTime_greedy_s)
for h in range(task_number1): 
    '搜索' 
    if __name__ == '__main__':
        waiting_task_number=h#得出小于给定task 3的最大节点序号
        r=search(array3, template_task_tables[waiting_task_number]['st'][0])    
    node_result=[]#找出需要计算的树节点
    for i in range(r+1):
        node_result.append(array3[i])        
    '节点内计算'
    #节点内worker i空闲时段的结束时间排序
    temp_tree_node_worker_order={}
    tree_node_worker_order={}#{树节点：{worker number:空闲时段的结束时间}}
    for i in node_result:
        temp_tree_node_worker_order[i]={}
        for j in node_worker[i]:
            temp_tree_node_worker_order[i][j]=template_worker_tables[j]['idle_period'][i][2]
        tree_node_worker_order[i]=sorted(temp_tree_node_worker_order[i].items(), key=lambda kv:(kv[1], kv[0]),reverse=True)   

    #根据任务，在节点内查找距离最近的满足约束的worker
    l=len(tree_node_worker_order)
    utility=0
    if (l>0):  
        task_endtime=(template_task_tables[waiting_task_number]['st'][0]+template_task_tables[waiting_task_number]['pt'][0])
        '针对不同的树节点,可以采用并行'
        for i in tree_node_worker_order:
            n=len(tree_node_worker_order[i])
            for j in range(n):#针对节点内不同worker的空闲时段，比较前后任务的时间约束是否满足，比较距离远近
                if (task_endtime<=tree_node_worker_order[i][j][1]):
                    temp_worker=tree_node_worker_order[i][j][0]
                    temp_idletime=i
                    #比较运动到task waiting_task_number所需时间
                    if (template_worker_tables[temp_worker]['idle_period'][temp_idletime][0]==[]):#当temp_worker在空闲时段开始前temp_idletime没有任务的情况
                        wx1=template_worker_tables[temp_worker]['location'][0]                   
                        wy1=template_worker_tables[temp_worker]['location'][1]  
                    else:
                        temp_task1=template_worker_tables[temp_worker]['idle_period'][temp_idletime][0][0]
                        wx1=template_task_tables[temp_task1]['sl'][0]
                        wy1=template_task_tables[temp_task1]['sl'][1]
                    slx=template_task_tables[waiting_task_number]['sl'][0]
                    sly=template_task_tables[waiting_task_number]['sl'][1]
                    distance1=round((((wx1-slx)**2+(wy1-sly)**2)**(1/2)),3)
#                    distance1=haversine(wy1,wx1,sly,slx)
                    move_time1=round(distance1/template_worker_tables[temp_worker]['speed'][0],3)
                    start_time1=i
                    #比较运动到下一个task所需时间
                    if (template_worker_tables[temp_worker]['idle_period'][temp_idletime][3]!=[]):
                        temp_task2=template_worker_tables[temp_worker]['idle_period'][temp_idletime][3][0]
                        wx2=template_task_tables[temp_task2]['sl'][0]
                        wy2=template_task_tables[temp_task2]['sl'][1]
                        dlx=template_task_tables[waiting_task_number]['dl'][0]
                        dly=template_task_tables[waiting_task_number]['dl'][1]
                        distance2=round((((wx2-dlx)**2+(wy2-dly)**2)**(1/2)),3)
#                        distance2=haversine(wy2,wx2,dly,dlx)
                        distance0=round((((wx2-wx1)**2+(wy2-wy1)**2)**(1/2)),3)#计算插入task之前需要的travel distance 
#                       distance0=haversine(wy2,wx2,wy1,wx1)
                    else:
                        distance2=0
                        distance0=0
                    differe_distance=distance2+distance1-distance0#计算插入任务后travel distance的变化
                    move_time2=round(distance2/template_worker_tables[temp_worker]['speed'][0],3)
                    start_time2=template_task_tables[waiting_task_number]['st'][0]+template_task_tables[waiting_task_number]['pt'][0]
#                    limit_time2=template_worker_tables[temp_worker]['idle_period'][temp_idletime][2]
                    if ((move_time1+start_time1)<template_task_tables[waiting_task_number]['st'][0]):
                        if ((move_time2+start_time2)<tree_node_worker_order[i][j][1]):
                            if utility<template_task_tables[waiting_task_number]['budget'][0]-constant*differe_distance:#比较距离,选择最优距离的worker插入
                                utility=template_task_tables[waiting_task_number]['budget'][0]-constant*differe_distance#最终插入的worker的距离
                                incert_worker=tree_node_worker_order[i][j][0]#最终插入的worker
                                incert_tree_node=i
#                                    break#加上break是随机选择树节点内第一个满足条件的worker插入，没有break则表示选取节点内距离最优的worker
                else:
                    break
    '更新'
    if (utility>0):#当distance=10000时，表示没有找到满足的worker插入,distance<1表示设置的阈值
        #更新worker的schedule，idle_period；
        #对于实时订单，是直接在该空闲时段的起始位置增加完成实时订单所需整体时间，得到一个新的空闲时段，即该空闲时段的起点改变了；要么就是增加一段空闲时段，即从原有空闲时段起点至任务起始位置所需时段。
        template_worker_tables[incert_worker]['schedule'].append(waiting_task_number)
        pre_idle=listcopy(template_worker_tables[incert_worker]['idle_period'][incert_tree_node])
        template_worker_tables[incert_worker]['idle_period'][incert_tree_node][2]=template_task_tables[waiting_task_number]['st'][0]
        template_worker_tables[incert_worker]['idle_period'][incert_tree_node][3]=[waiting_task_number]
        new_tree_node=template_task_tables[waiting_task_number]['st'][0]+template_task_tables[waiting_task_number]['pt'][0]
        template_worker_tables[incert_worker]['idle_period'][new_tree_node]=[[waiting_task_number],new_tree_node,pre_idle[2],pre_idle[3]]
        #更新tree_node，node_worker
        if (new_tree_node not in tree_node):
            tree_node.append(new_tree_node)
            node_worker[new_tree_node]=[incert_worker]
        else:
            node_worker[new_tree_node].append(incert_worker)
        root3.insert(new_tree_node)
        array3=root3.inorderTraversal(root3) 
        total_utility+=utility

print('memory=',tracemalloc.get_traced_memory())
M1,M2=tracemalloc.get_traced_memory()
tracemalloc.stop()
'时间测试'
dateTime_e=time.time() #获取当前时间戳
dateTime_e=datetime.datetime.fromtimestamp(dateTime_e) #将时间戳转换为日期
print('total running time=',dateTime_e-dateTime_s)
running_time=(dateTime_e-dateTime_s).total_seconds()
complete_task=0
for v in template_worker_tables:
    complete_task+=len(template_worker_tables[v]['schedule'])
complete_task=complete_task
rate=complete_task/len(task_tables)
h1=0#统计被使用的worker数目
h2=0
for i in template_worker_tables:
    if (len(template_worker_tables[i]['schedule'])!=0):
        h1+=1
for i in worker_tables2:
    if (len(worker_tables2[i]['schedule'])!=0):
        h2+=1
#print('G1=',G1)
#print('G2=',G2)
#print('G3=',G3)
#print('rate=',rate)
#result=[G1,G2,G3,result_time,rate]
result=[complete_task,rate,total_utility,M1,M2,running_time]
with open('result1_2.csv','w',newline='')as csv_file:
    # 获取一个csv对象进行内容写入
    writer=csv.writer(csv_file)
    for row in result:
        # writerow 写入一行数据
        writer.writerow([row])
print('result=',result)
