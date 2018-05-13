# -*- coding: utf-8 -*-
"""
Created on Sat Dec 16 11:47:32 2017

@author: elara
"""


import platform
import pandas as pd
from pandasql import sqldf 
import copy
import multiprocessing
cores = multiprocessing.cpu_count()
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
merged_corpus_path = main_path+ 'Elara/Documents/paper/merged_corpus/merged_kv_result.csv'


# 载入
print('load merge corpus')
merge_corpus = pd.read_csv(merged_corpus_path, names=['name','date','title','url','content','cate','source','conments','uv','url_ch',\
                                                     'volume','is_split_2016_1','is_split_2016_2','is_split_2016_3',\
                                                     'is_split_all_1','is_split_all_2','is_split_all_3'])
print('nrow = ',len(merge_corpus))

q1 = 'select date, count(url) as news_cnt from (select distinct date,url,title,content,cate,source,conments,uv,url_ch from merge_corpus where content is not null and title is not null) a group by date'
print('running: \n',q1)
date_cnt = pysqldf(q1)
date_cnt.to_csv(main_path+'Elara/Documents/paper/document_num/'+'date_cnt.csv',encoding='utf-8',header=False,index=False)


q2 = 'select name, date, count(url) as news_cnt from (select distinct name, date,url,title,content,cate,source,conments,uv,url_ch from merge_corpus where content is not null and title is not null) a group by name,date'
print('running: \n',q2)
name_date_cnt = pysqldf(q2)
name_date_cnt.to_csv(main_path+'Elara/Documents/paper/document_num/'+'name_date_cnt.csv',encoding='utf-8',header=False,index=False)

def decode_tfdf(kv_text):
    # 输入字符串 k:v
    d= {}
    if kv_text == None:
        print('decode_tf: input kv_text is null')
        return d
    else:
        for i in kv_text.split():
            d[i.split(':')[0]] = int(i.split(':')[1])
    return d

def cal_d_len(d):
    return sum(d.values())

def cal_len(kv_text):
    return cal_d_len(decode_tfdf(kv_text))

def get_len(record_list):
    r = copy.deepcopy(record_list)
    r[3] = cal_len(r[3])
    r[4] = cal_len(r[4])
    return r
     

q3 = 'select distinct name, date,url,title,content from merge_corpus where content is not null and title is not null'
print('running: \n',q3)
corpus = pysqldf(q3)

print('convert to list')
corpus_list = [list(corpus.iloc[i,:]) for i in range(len(corpus))]

print('start mapping')
pool = multiprocessing.Pool(processes = cores)
result = pool.map(get_len,corpus_list)
pool.close()

print('saving')
pd.DataFrame(result).to_csv(main_path+'Elara/Documents/paper/document_num/'+'doc_len.csv',encoding='utf-8',header=False,index=False)