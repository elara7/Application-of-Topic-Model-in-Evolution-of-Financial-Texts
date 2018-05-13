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

all_frequency_dict={}
for i in range(len(kv_corpus)):
    for j in list(kv_corpus.iloc[i,:])[0].split():
        word = j.split(':')[0]
        fre = j.split(':')[1]
        if word in all_frequency_dict.keys():
            all_frequency_dict[word] += int(fre)
        else:
            all_frequency_dict[word] = int(fre)
 
if __name__ ==  '__main__':
    
#i = os.listdir(main_path+'Elara/Documents/paper/seg/pos_unfilter/')[1]
    for i in os.listdir(main_path+'Elara/Documents/paper/4_seg/pos_unfilter/'):
        print('loading',i)
        temp = pd.read_csv(main_path+"Elara/Documents/paper/4_seg/pos_unfilter/"+i,
                               names=['name','title','url','date','content','cate','source','conments','uv','url_ch'], 
                               engine='python',
                               encoding="utf-8")
        temp2 = [' '.join(list(temp.iloc[j,[1,4]])) for j in range(len(temp))]
        temp2 = [[j[1]+' '+j[4] for j in temp.iloc[i,:]] for i in range(len(temp))]
        temp.iloc[1,:]
        print('load',i,' done')
        pre_cnt = [[i[0],i[1],len(i[4].split())] for i in temp2]
        
        print('clear_record ',i)
        pool = multiprocessing.Pool(processes = cores)
        res = pool.map(clear_record, temp2)
        pool.close()
        print('clear_record ',i,' done')
        post_cnt = [[i[0],i[1],len(i[4].split())] for i in res]
        
        
        result = pd.DataFrame(res)
        result.to_csv(main_path + 'Elara/Documents/paper/filter/corpus/' + i,encoding='utf-8',header=False,index=False)
        
        pre_cnt = pd.DataFrame(pre_cnt)
        pre_cnt.to_csv(main_path + 'Elara/Documents/paper/filter/cnt/pre_cnt' + i,encoding='utf-8',header=False,index=False)
        
        post_cnt = pd.DataFrame(post_cnt)
        post_cnt.to_csv(main_path + 'Elara/Documents/paper/filter/cnt/post_cnt' + i,encoding='utf-8',header=False,index=False)
        print('save',i,' done')
        
    

