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

# kv化
def kv_text(text):
    if text==None:
        #print('input text is None')
        return text
    if text==0:
        print('input text is 0')
        return text
    if len(text)==0:
        print('input text length is 0')
        return text
    else:
        d = {}
        text = text.split()
        for i in text:
            if i in d.keys():
                d[i] += 1
            else:
                d[i] = 1
        return ' '.join([':'.join([i,str(d[i])]) for i in d])


def kv_record(record):
    record[2] = kv_text(record[2])
    record[4] = kv_text(record[4])
    return record

if __name__ == '__main__':
    split_corpus_merge_list = [list(split_corpus_merge.iloc[i,:]) for i in range(len(split_corpus_merge))]
    print('convert to list')
    
    print('start kv')
    pool = mp.Pool(processes = cores)
    result = pool.map(kv_record,split_corpus_merge_list)
    pool.close()
    print(split_corpus_merge_list[500])
    print(result[500])
    ###################################################################################################################################################
    
    ## 保存
    
    pd.DataFrame(result).to_csv(main_path+'Elara/Documents/paper/8_merged_corpus/'+'merged_kv_result.csv',encoding='utf-8',header=False,index=False)
    print('save')