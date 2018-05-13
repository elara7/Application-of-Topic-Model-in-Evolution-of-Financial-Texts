# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 19:59:22 2017

@author: elara
"""
import platform
import pandas as pd
import os
import multiprocessing 
import sys
import csv
import copy
import pickle
import math
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
        
cores=multiprocessing.cpu_count()*2
print('set cores = ' ,cores)

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'


def cal_line(line):
    word_pos = {}
    word_len = {}
    line = [i.split('_') for i in line.split()]
    for i in line:
        word = len(i[0])
        pos = i[1]
        if pos in word_pos.keys():
            word_pos[pos] += 1
        else:
            word_pos[pos] = 1
        if word in word_len.keys():
            word_len[word] += 1
        else:
            word_len[word] = 1
        doc_len = len(line)
    return [word_pos,word_len,doc_len]

# 词性：词数  词长度：词数  文章长度：文章数 
def merge_dict_y2x(x,y):
    # 输入2个dict
    for k, v in y.items():
        if k in x.keys():
            #print(k,' already in x')
            x[k] += v
        else:
            x[k] = v
    return x
            
def merge_dict(dict_list):
    # 输入1个list，里面每个元素是dict
    res = {}
    for i in dict_list:
        res = merge_dict_y2x(res,i)
    return res
    


content_all = []

for i in os.listdir(main_path+'Elara/Documents/paper/4_seg/pos_unfilter/'):
    print('loading',i)
    temp = pd.read_csv(main_path+"Elara/Documents/paper/4_seg/pos_unfilter/"+i,
                           names=['name','title','url','date','content','cate','source','conments','uv','url_ch'], 
                           engine='python',
                           encoding="utf-8")
    temp2 = [[j for j in temp.iloc[i,:]] for i in range(len(temp))]
    print('load',i,' done')
    if len(content_all)==0:
        content_all = copy.deepcopy([ i[1]+' '+i[4] for i in temp2 if type(i[4]) == str ])
    else:
        content_all += copy.deepcopy([ i[1]+' '+i[4] for i in temp2 if type(i[4]) == str ])

print('unique')
content = list(set(content_all))
content =[i for i in content if i!=None]
content =[i for i in content if len(i)!=0]

print('cal')


pool = multiprocessing.Pool(processes = cores)
res = pool.map(cal_line, content)
pool.close()

print('merge')
word_pos_dict =[i[0] for i in res]
word_len_dict =[i[1] for i in res]
doc_len =[i[2] for i in res]

word_pos_dicts = merge_dict(word_pos_dict)
word_len_dicts = merge_dict(word_len_dict)

print('saving')
f1 = open(main_path+ 'Elara/Documents/paper/4_seg/seg_ana/corpus_ana/res/word_pos_dicts.txt',"wb")
pickle.dump(word_pos_dicts, f1)
f1.close()

f1 = open(main_path+ 'Elara/Documents/paper/4_seg/seg_ana/corpus_ana/res/word_len_dicts.txt',"wb")
pickle.dump(word_len_dicts, f1)
f1.close()

f1 = open(main_path+ 'Elara/Documents/paper/4_seg/seg_ana/corpus_ana/res/doc_len.txt',"wb")
pickle.dump(doc_len, f1)
f1.close()