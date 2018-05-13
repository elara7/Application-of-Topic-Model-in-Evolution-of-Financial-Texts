# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 11:15:03 2017

@author: elara
"""

import platform
import pandas as pd
from pandasql import sqldf 
import multiprocessing as mp
cores = mp.cpu_count()
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
split_corpus_path = main_path+ 'Elara/Documents/paper/7_split_corpus/split_result.csv'


# 载入
split_corpus = pd.read_csv(split_corpus_path, names=['name','date','title','url','content','cate','source','conments','uv','url_ch',\
                                                     'volume','is_split'])
print('loaded')
###################################################################################################################################################
# 去重
split_corpus_distinct = split_corpus.drop_duplicates().sort_values(by=['name','date','url'])
print('drop_duplicates')
###################################################################################################################################################
# 合并
## 检查待合并数据
#q1 = 'select name,date,url,count(1) from split_corpus_distinct group by name,date,url having count(1) > 1'
#duplicates_res = pysqldf(q1)
#
#q2 = 'select b.*\
#from \
#( \
#select * from duplicates_res \
#) a \
#left outer join \
#( \
#select * from split_corpus_distinct \
#) b \
#on a.name=b.name and a.date=b.date and a.url=b.url'
#duplicates_record = pysqldf(q2)

## 合并
q3 = 'select \
name,date,title,url,\
replace(group_concat(content),","," ") as content,\
cate,source,conments,uv,url_ch,\
volume,\
is_split \
from split_corpus_distinct \
group by \
name,date,title,url,cate,source,conments,uv,url_ch,\
volume,\
is_split'
split_corpus_merge = pysqldf(q3)
print('merge')
## 检查合并后原分裂数据
#q4 = 'select b.*\
#from \
#( \
#select * from duplicates_res \
#) a \
#left outer join \
#( \
#select * from split_corpus_merge \
#) b \
#on a.name=b.name and a.date=b.date and a.url=b.url'
#split_corpus_merge_check = pysqldf(q4)
#check_text = split_corpus_merge_check.iloc[0,4]

##检查是否还有重复
#IsDuplicated = split_corpus_merge.duplicated(['name','date','url']) 
#IsDuplicated[IsDuplicated==True]

###################################################################################################################################################

## 保存

split_corpus_merge.to_csv(main_path+'Elara/Documents/paper/8_merged_corpus/'+'merged_result.csv',encoding='utf-8',header=False,index=False)
print('save')