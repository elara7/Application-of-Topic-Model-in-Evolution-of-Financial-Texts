# -*- coding: utf-8 -*-
"""
Created on Tue Jan  2 17:09:22 2018

@author: elara
"""


import platform
import codecs
import pandas as pd
import os
import traceback
import itertools
import multiprocessing 
import sys
import csv
import re
maxInt = sys.maxsize
decrement = True

while decrement:
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.

    decrement = False
    try:
        csv.field_size_limit(maxInt)
    except OverflowError:
        maxInt = int(maxInt/10)
        decrement = True
        
cores=multiprocessing.cpu_count()
print('set cores = ' ,cores)

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'

def cal_line(line_all):
    name = line_all[0]
    line = line_all[1]
    word_freq = {}
    line = [i.split('_') for i in line.split()]
    for i in line:
        word = i[0]

        if word in word_freq.keys():
            word_freq[word] += 1
        else:
            word_freq[word] = 1
    return [name,' '.join([str(x)+':'+str(y) for x,y in word_freq.items()])]

    
 
if __name__ ==  '__main__':
    res = []
    pool = multiprocessing.Pool(processes = cores)
#i = os.listdir(main_path+'Elara/Documents/paper/seg/pos_unfilter/')[1]
    for i in os.listdir(main_path+'Elara/Documents/paper/4_seg/pos_unfilter/'):
        print('loading',i)
        temp = pd.read_csv(main_path+"Elara/Documents/paper/4_seg/pos_unfilter/"+i,
                               names=['name','title','url','date','content','cate','source','conments','uv','url_ch'], 
                               engine='python',
                               encoding="utf-8")
        

        temp_2 = [[temp['name'].iloc[a],temp['title'].iloc[a] +' '+ temp['content'].iloc[a]] for a in range(len(temp)) if type(temp['content'].iloc[a])==str]
        
        

        res = res + pool.map(cal_line, temp_2)

    pd.DataFrame(res).to_csv(main_path+'Elara/Documents/paper/4_seg/kv/kv.csv',header=None,index=None)
    

