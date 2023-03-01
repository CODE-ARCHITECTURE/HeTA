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
'存储测试'
tracemalloc.start()

total_utility=0
x+=1    
template_worker_tables=deepcopy(worker_tables)
print("template_worker_tables=",template_worker_tables[0])
print("template_task_tables=",template_task_tables[0])

for t in range(len(template_task_tables)):
    optimal_distance = 100
    optimal_worker = 10000
    optimal_location = 10000
    insert_flag = 0
    for w in range(len(template_worker_tables)):
        if (len(template_worker_tables[w]['schedule']) == 0):
            #当没有包含任务时，判断是否满足插入条件，计算结果（最短距离，插入工人索引w，‘schedule’中的位置l）
            distance = round((((template_worker_tables[w]['location'][0]-template_task_tables[t]['sl'][0])**2+(template_worker_tables[w]['location'][1]-template_task_tables[t]['sl'][1])**2)**(1/2)),3)
            move_time = round(distance/template_worker_tables[w]['speed'][0],3)
            if (move_time <= template_task_tables[t]['st'][0]):
                if (distance < optimal_distance):
                    optimal_distance = distance
                    optimal_worker = w
                    optimal_location = 0
                    insert_flag = 1
        else:
            for l in range(len(template_worker_tables[w]['schedule'])+1):
                if (l == 0):
                    #判断第一个任务l=0之前的空段是否满足[工人w，任务t，任务l]，并计算（最短距离，插入工人索引w，‘schedule’中的位置l）
                    temp_t = template_worker_tables[w]['schedule'][l]
                    distance1 = round((((template_worker_tables[w]['location'][0]-template_task_tables[t]['sl'][0])**2+(template_worker_tables[w]['location'][1]-template_task_tables[t]['sl'][1])**2)**(1/2)),3)
                    move_time1 = round(distance1/template_worker_tables[w]['speed'][0],3)
                    if (move_time1 <= template_task_tables[t]['st'][0]):
                        distance2 = round((((template_task_tables[t]['dl'][0]-template_task_tables[temp_t]['sl'][0])**2+(template_task_tables[t]['dl'][1]-template_task_tables[temp_t]['sl'][1])**2)**(1/2)),3)
                        move_time2 = round(distance2/template_worker_tables[w]['speed'][0],3)+template_task_tables[t]['st'][0]+template_task_tables[t]['pt'][0]
                        if (move_time2 <= template_task_tables[temp_t]['st'][0]):
                            distance0 = round((((template_worker_tables[w]['location'][0]-template_task_tables[temp_t]['sl'][0])**2+(template_worker_tables[w]['location'][1]-template_task_tables[temp_t]['sl'][1])**2)**(1/2)),3)
                            distance = distance1 + distance2 - distance0
                            if (distance < optimal_distance):
                                optimal_distance = distance
                                optimal_worker = w
                                optimal_location = 0 
                                insert_flag = 1
                elif (l == len(template_worker_tables[w]['schedule'])):
                    #判断最后一个任务l=len-1之后的空段是否满足[任务l，任务t]，并计算（最短距离，插入工人索引w，‘schedule’中的位置l）
                    temp_t = template_worker_tables[w]['schedule'][l-1]
                    distance = round((((template_task_tables[t]['sl'][0]-template_task_tables[temp_t]['dl'][0])**2+(template_task_tables[t]['sl'][1]-template_task_tables[temp_t]['dl'][1])**2)**(1/2)),3)
                    move_time = round(distance/template_worker_tables[w]['speed'][0],3) + template_task_tables[temp_t]['st'][0]+template_task_tables[temp_t]['pt'][0]
                    if (move_time <= template_task_tables[t]['st'][0]):
                        if (distance < optimal_distance):
                            optimal_distance = distance
                            optimal_worker = w
                            optimal_location = l
                            insert_flag = 1
                else:
                    #判断两个任务l-1与l之间的空段是否满足[任务l，任务t，任务l+1]，并计算（最短距离，插入工人索引w，‘schedule’中的位置l）
                    temp_t1 = template_worker_tables[w]['schedule'][l-1]
                    temp_t2 = template_worker_tables[w]['schedule'][l]
                    distance1 = round((((template_task_tables[t]['sl'][0]-template_task_tables[temp_t1]['dl'][0])**2+(template_task_tables[t]['sl'][1]-template_task_tables[temp_t1]['dl'][1])**2)**(1/2)),3)
                    move_time1 = round(distance1/template_worker_tables[w]['speed'][0],3) + template_task_tables[temp_t1]['st'][0]+template_task_tables[temp_t1]['pt'][0]
                    if (move_time1 <= template_task_tables[t]['st'][0]):
                        distance2 = round((((template_task_tables[t]['dl'][0]-template_task_tables[temp_t2]['sl'][0])**2+(template_task_tables[t]['dl'][1]-template_task_tables[temp_t2]['sl'][1])**2)**(1/2)),3)
                        move_time2 = round(distance2/template_worker_tables[w]['speed'][0],3) + template_task_tables[t]['st'][0]+template_task_tables[t]['pt'][0]
                        if (move_time2 <= template_task_tables[t]['st'][0]):
                            distance0 = round((((template_task_tables[temp_t2]['sl'][0]-template_task_tables[temp_t1]['dl'][0])**2+(template_task_tables[temp_t2]['sl'][1]-template_task_tables[temp_t1]['dl'][1])**2)**(1/2)),3)
                            distance = distance1 + distance2 - distance0
                            if (distance < optimal_distance):
                                optimal_distance = distance
                                optimal_worker = w
                                optimal_location = l 
                                insert_flag = 1
    #确定任务t的插入位置，更新工人schedule信息
    if (insert_flag == 1):
        number_task = len(template_worker_tables[optimal_worker]['schedule'])
        if (number_task == 0 or number_task == optimal_location):
            template_worker_tables[optimal_worker]['schedule'].append(t)
        else:
            template_worker_tables[optimal_worker]['schedule'].insert(optimal_location, t)
            
'存储测试'
print('memory=',tracemalloc.get_traced_memory())
tracemalloc.stop()
'时间测试'
dateTime_e=time.time() #获取当前时间戳
dateTime_e=datetime.datetime.fromtimestamp(dateTime_e) #将时间戳转换为日期
running_time=(dateTime_e-dateTime_s).total_seconds()
print('total running time=',running_time)
'统计完成的任务数目'
completed_task = 0
for w in range(len(template_worker_tables)):
    completed_task = completed_task + len(template_worker_tables[w]['schedule'])
print('total completed tasks=',completed_task)














