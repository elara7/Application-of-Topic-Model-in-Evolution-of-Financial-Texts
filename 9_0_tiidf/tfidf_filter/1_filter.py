# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 14:33:23 2017

@author: elara
"""


import platform
import pandas as pd
from pandasql import sqldf 
import re
import multiprocessing
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
merged_corpus_path = main_path+ 'Elara/Documents/paper/8_merged_corpus/merged_result.csv'
merged_kv_corpus_path = main_path+ 'Elara/Documents/paper/8_merged_corpus/merged_kv_result.csv'




def word_filter(word):
    return re.search(r'^[0-9]+$',word)

def filter_line(line):
    name = line[0]
    stage = line[11]
    date = line[1]
    if type(line[2])!=str:
        content = line[4]
    else:
        content = ' '.join([line[2],line[4]])
    freq = {}
    temp = content.split()
    for i in temp:
        if i in freq.keys():
            freq[i]+=1
        else:
            freq[i]=1
    stopword  = [word for word,freq in freq.items() if freq<=0]
    res = []
    for i in temp:
        if word_filter(i)!=None or i in stopword:
            continue
        else:
            res.append(i)
    if len(res)<10:
        res=None
    else:
        res = ' '.join(res)
    return [name]+[date]+[stage]+[res]
# 载入
print('load merge corpus')
merge_corpus = pd.read_csv(merged_corpus_path, names=['name','date','title','url','content','cate','source','conments','uv','url_ch',\
                                                     'volume','is_split'])
print('nrow = ',len(merge_corpus))


stock_names = list(set(merge_corpus['name']))
cores = int(multiprocessing.cpu_count())
pool = multiprocessing.Pool(processes = cores)
for n in stock_names:
    q0 = 'select distinct * from merge_corpus where content is not null and name = "'+n+'"'
    print('running ',q0)
    texts = pysqldf(q0)
    
    textslist = [list(texts.iloc[i,:]) for i in range(len(texts))]
    res = pool.map(filter_line,textslist)
    pd.DataFrame(res).to_csv(main_path + 'Elara/Documents/paper/9_0_tiidf/tfidf_filter/filterresult/'+n+'.csv',encoding='utf-8',header=False,index=False)