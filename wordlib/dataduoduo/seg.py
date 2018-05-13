# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 12:27:01 2017

@author: elara
"""
import pandas as pd
import os
import thulac
import platform
import multiprocessing 
import math
import copy
import traceback
import re
from pandasql import sqldf 
import codecs
pysqldf = lambda q: sqldf(q, globals())

cores=multiprocessing.cpu_count()
print('set cores = ' ,cores)

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
max_len = 50000
model_path_3 = main_path+'Elara/Documents/THULAC_pro_c++_v1/models/'
model_path_2 = main_path+'Elara/Documents/Models_v1_v2/models/'

thu = thulac.thulac(model_path=model_path_3, T2S=False, seg_only=False, filt=False)

f = codecs.open(main_path + 'Elara/Documents/wordlib/dataduoduo/中文分词数据_词库_财经金融词汇大全（47976条）.txt',encoding='gbk')
user_word = [i.split()[0].strip() for i in f.readlines()]
f.close()
u = []
s = []
ok_list= ['n','np','ns','ni','nz']
for i in user_word:
    res = thu.fast_cut(i,text=True).split()
    cnt = 0
    cl = len(res)
    for j in res:
        temp = j.split('_')
        if temp[1] not in ok_list:
            cnt += 1
        else:
            pass
    if cnt == cl:
        u.append(i)
    else:
        s.append(i)
        
f = codecs.open(main_path + 'Elara/Documents/wordlib/dataduoduo/new.txt','w',encoding='utf-8')
for i in u:
    f.write(i+'\n')
f.close()

f = codecs.open(main_path + 'Elara/Documents/wordlib/dataduoduo/new_s.txt','w',encoding='utf-8')
for i in s:
    f.write(i+'\n')
f.close()