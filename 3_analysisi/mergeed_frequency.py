# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 13:09:02 2017

@author: elara
"""



import platform
import pandas as pd
from pandasql import sqldf 
import pickle
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
merged_corpus_path = main_path+ 'Elara/Documents/paper/8_merged_corpus/merged_result.csv'
merged_kv_corpus_path = main_path+ 'Elara/Documents/paper/8_merged_corpus/merged_kv_result.csv'


print('load merge kv corpus')

    
merge_kv_corpus = pd.read_csv(merged_kv_corpus_path, names=['name','date','title','url','content','cate','source','conments','uv','url_ch',\
                                                     'volume','is_split'])
print('nrow = ',len(merge_kv_corpus))


q1 = 'select distinct content from merge_kv_corpus where content is not null'
print('running ',q1)
kv_corpus = pysqldf(q1)

print('counting')
all_frequency_dict={}
for i in range(len(kv_corpus)):
    for j in list(kv_corpus.iloc[i,:])[0].split():
        word = j.split(':')[0]
        fre = j.split(':')[1]
        if word in all_frequency_dict.keys():
            all_frequency_dict[word] += int(fre)
        else:
            all_frequency_dict[word] = int(fre)
      
all_frequency = pd.DataFrame([[i1,j1] for i1,j1 in all_frequency_dict.items()])
all_frequency.columns=['word','cnt']
all_frequency = all_frequency.sort_values(by='cnt',ascending=False)
all_frequency.to_csv(main_path+'Elara/Documents/paper/3_analysisi/frequency_table/merged_frequency_all.csv',header=None,index=None)

names = list(set(merge_kv_corpus['name']))



for n in range(len(names)):
    q1 = 'select distinct content from merge_kv_corpus where content is not null and name = "' + names[n]+'"'
    print('running ',q1)
    kv_corpus = pysqldf(q1)
    
    print('counting')
    all_frequency_dict={}
    for i in range(len(kv_corpus)):
        for j in list(kv_corpus.iloc[i,:])[0].split():
            word = j.split(':')[0]
            fre = j.split(':')[1]
            if word in all_frequency_dict.keys():
                all_frequency_dict[word] += int(fre)
            else:
                all_frequency_dict[word] = int(fre)
          
    all_frequency = pd.DataFrame([[i1,j1] for i1,j1 in all_frequency_dict.items()])
    all_frequency.columns=['word','cnt']
    all_frequency = all_frequency.sort_values(by='cnt',ascending=False)
    all_frequency.to_csv(main_path+'Elara/Documents/paper/3_analysisi/frequency_table/stocks/merged_frequency_'+names[n]+'.csv',header=None,index=None)


