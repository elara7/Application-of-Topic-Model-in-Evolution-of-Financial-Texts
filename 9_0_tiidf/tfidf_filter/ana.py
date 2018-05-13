# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 13:09:02 2017

@author: elara
"""



import platform
import pandas as pd
from pandasql import sqldf 
import datetime
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
corpus_path = main_path+ 'Elara/Documents/paper/9_0_tiidf/tfidf_filter/filterresult/上汽集团.csv'


print('load merge kv corpus')

    
corpus = pd.read_csv(corpus_path, names=['name','date','is_split','content'],engine='python',encoding='utf-8')
print('nrow = ',len(corpus))




names = list(set(merge_kv_corpus['name']))



for n in range(len(names)):
    #q0 = 'select distinct date,content,is_split from merge_kv_corpus where content is not null and name = "' + names[n]+'"'
    q0 = 'select distinct date,content,is_split from corpus where content is not null'
    print('running ',q0)
    kv_corpus = pysqldf(q0)
    
    #q1 = 'select is_split,avg(volume) as avg_volume from (select distinct is_split,date,volume from merge_kv_corpus where content is not null and name = "上汽集团") a  group by is_split'
    q2 = 'select is_split,count(1) as cnt from kv_corpus group by is_split'
    q3 = 'select is_split,max(date) as max_date from kv_corpus group by is_split'
    q4 = 'select is_split,min(date) as min_date from kv_corpus group by is_split'
    #print('running ',q1)
    #vol = pysqldf(q1)
    print('running ',q2)
    doc_cnt = pysqldf(q2)
    print('running ',q3)
    max_date = pysqldf(q3)
    print('running ',q4)
    min_date = pysqldf(q4)
    
    q5='\
    select \
    a.is_split as is_split,\
    b.cnt as cnt,\
    c.max_date as max_date,\
    d.min_date as min_date \
    from  \
    ( \
     select distinct  \
     is_split \
     from kv_corpus \
    ) a \
    left outer join \
    ( \
     select is_split,cnt  \
     from doc_cnt \
    ) b \
     on a.is_split=b.is_split \
    left outer join \
    ( \
     select is_split,max_date \
     from max_date \
    ) c \
     on a.is_split=c.is_split \
    left outer join \
    ( \
     select is_split,min_date  \
     from min_date \
    ) d \
     on a.is_split=d.is_split \
     '
    print('running ',q5)
    resdf = pysqldf(q5) 
    
    day_diff = pd.DataFrame([[resdf['is_split'].iloc[i],(datetime.datetime.strptime(str(resdf['max_date'].iloc[i]),'%Y%m%d')  - datetime.datetime.strptime(str(resdf['min_date'].iloc[i]),'%Y%m%d')).days] for i in range(len(resdf))])
    day_diff.columns=['is_split','day_diff']
    
    q6 = 'select a.is_split as is_split, a.cnt as cnt, a.min_date as min_date ,a.max_date as max_date ,b.day_diff+1 as day_diff, (a.cnt*0.1)/((b.day_diff+1)*0.1) as avg_cnt from (select * from resdf) a left outer join (select * from day_diff) b on a.is_split = b.is_split'
    print('running ',q6)
    resdf = pysqldf(q6) 
    
    resdf.to_csv(main_path+'Elara/Documents/paper/3_analysisi/daily_doc_cnt/'+names[n]+'.csv',header=None,index=None)


