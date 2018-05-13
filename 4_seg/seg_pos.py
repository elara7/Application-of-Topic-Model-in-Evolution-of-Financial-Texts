# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 12:27:01 2017

@author: elara
"""
import pandas as pd
import os
import thulac
import platform
import multiprocessing 
import math
import copy
import traceback
import re
from pandasql import sqldf 
pysqldf = lambda q: sqldf(q, globals())

cores=multiprocessing.cpu_count()
print('set cores = ' ,cores)

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
max_len = 50000
model_path_3 = main_path+'Elara/Documents/THULAC_pro_c++_v1/models/'
model_path_2 = main_path+'Elara/Documents/Models_v1_v2/models/'
user_word_path = main_path+'Elara/Documents/wordlib/user_word.txt'

thu = thulac.thulac(user_dict=user_word_path, model_path=model_path_3, T2S=False, seg_only=False, filt=False)
# 读取文本


#基于标注分词，不保存词性
#def get_seg(test):
#    try:
#        #print('succeed')
#        return ' '.join([i.split('_')[0] for i in thu.fast_cut(test,text=True).split()])
#    except:
#        traceback.print_exc()
#        return None

#基于标注分词，保存词性
def split_line(line):
    res = []
    len_i = len(line)
    k = math.ceil(len_i/max_len)
    l = math.ceil(len_i/k)
    x = 0
    y = x + l
    for j in range(k):
        if x >= len_i:
            break
        if y > len_i:
            y = len_i
        res.append(line[x:y])
        x = y
        y = x + l
    return res
    
def remove_punctuation(line):
    rule = re.compile(r"[^a-zA-Z0-9\u4e00-\u9fa5]")
    line = rule.sub(' ',line)
    line = ' '.join([i for i in line.split()])
    return line

def do_seg(test):
    try:
        #print('succeed')
        test = ' '.join(test.split())
        return thu.fast_cut(test,text=True)
    except:
        traceback.print_exc()
        return None

def get_seg(test):
    test = remove_punctuation(test)
    len_i = len(test)
    if len_i>max_len:
        print('size bigger than ',max_len)
        split_test = split_line(test)
        return ' '.join([do_seg(i) for i in split_test])
    else:
        return do_seg(test)

def seg(line):
    try:
        if len(line[4]) == 0 or line[4] == 0 or len(line[1]) == 0 or line[1] == 0:
            print(line[0],line[1],' title or content lenght is 0')
            return line
    except:
        if line[4] == None or line[1] == None:
            print(line[0],line[1],' title or content is None')
            return line
    try:
        title = get_seg(line[1])
        content = get_seg(line[4])
        if title == None:
            print(line[0],line[1],' title seg return None')
        if content == None:
            print(line[0],line[1],' content seg return None')
        line[1] = title
        line[4] = content
        return line 
    except:
        print(line[0],line[1],' error')
        return line

if __name__ ==  '__main__':
    
    pool = multiprocessing.Pool(processes = cores)

    for i in os.listdir(main_path+'Elara/Documents/paper/2_corpus/'):
        print('loading',i,' done')
        temp = pd.read_csv(main_path+"Elara/Documents/paper/2_corpus/"+i,
                                   skiprows=1,
                                   names=['index','title','url','date','content','cate','source','conments','uv'], 
                                   engine='python',
                                   encoding="utf-8").drop_duplicates().fillna(0).drop(['index'],axis=1)
        url_ch = pd.Series([i.split('.')[0].split('/')[-1] for i in temp['url']],name = 'url_ch',index=temp.index)
        name = pd.Series([i.split('.')[0].split('urllist')[1]]*len(temp),name='name',index=temp.index)
        temp = pd.concat([name,temp,url_ch],axis=1)
        
        q = 'select distinct name, title, url, date, content, cate, source, conments, uv, url_ch from temp where content is not null and title is not null and content != 0 and title != 0'
        print('running ',q)
        temp1 = pysqldf(q)
        content = [list(temp1.iloc[p,:]) for p in range(len(temp1)) if temp1.iloc[p,4]!=0]
    
                        
        print('load',i,' done')
        res = pool.map(seg, content)
        print('seg',i,' done')
        
        result = pd.DataFrame(res)
        result.to_csv(main_path + 'Elara/Documents/paper/4_seg/pos_unfilter/' + i,encoding='utf-8',header=False,index=False)
        print('save',i,' done')

