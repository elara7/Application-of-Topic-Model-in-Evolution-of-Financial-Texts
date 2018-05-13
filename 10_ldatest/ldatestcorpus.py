# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 18:28:50 2017

@author: elara
"""


import platform
import pandas as pd
from pandasql import sqldf 
import os
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
tfidf_filter_corpus_path = main_path+ 'Elara/Documents/paper/9_0_tiidf/tfidf_filter/filterresult/'
dataset = []
j = 1
for i in os.listdir(tfidf_filter_corpus_path):
    temp = pd.read_csv(tfidf_filter_corpus_path + i,encoding='utf-8',engine='python',names = ['stage','name','date','title','url','content','ch','source','url_ch'])
    if j ==1:
        dataset = temp
    else:
        dataset = dataset.append(temp)
    j+=1

q1 = 'select \
case when cnt >=5 and cnt <=10 then 1 \
when cnt >10 and cnt <=100 then 2 \
when cnt >100 and cnt <= 500 then 3 \
when cnt >500 and cnt <=1000 then 4 \
when cnt >1000 and cnt <= 2000 then 5 \
when cnt >2000 and cnt <= 3000 then 6 \
else 7 end as flag,\
name,stage,RANDOM() as r \
from \
(select name,stage,count(content) as cnt from dataset where content is not null group by name,stage) a where flag <7 order by r'
groupdata = pysqldf(q1)

groupdata = pysqldf(q1).groupby('flag').apply(lambda x: x.iloc[1,:])[['name','stage']]

test_data = []
x=1
for i,j in [list(groupdata[['name','stage']].iloc[i,:]) for i in range(len(groupdata))]:
    temp = dataset.loc[(dataset['name'] == i) & (dataset['stage'] == j),['content']]
    name = pd.Series([x]*len(temp),name='i',index=temp.index)
    temp = pd.concat([name,temp],axis=1)
    if len(test_data)==0:
        test_data = temp
    else:
        test_data = test_data.append(temp)
    x+=1

test_data.to_csv(main_path + 'Elara/Documents/paper/ldatest/test.csv',encoding='utf-8',header=None ,index=None)
