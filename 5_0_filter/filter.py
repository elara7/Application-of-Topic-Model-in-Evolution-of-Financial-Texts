# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 19:59:22 2017

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

# 词性剔除
stop_pos = ['w','m','q','mq','f','d','h','k','i','r','c','p','u','y','g','t','a','v']            
def del_pos(text):
    try:
        res_temp = []
        temp = [i.split('_') for i in text.split()]
        for i in temp:
            word = i[0].strip()
            pos = i[1]
            if pos in stop_pos:
                continue
            if re.search(r"[\u4e00-\u9fa5]",word) != None and pos != 'uw' and len(word) > 20:
                print(word,pos,len(word))
                continue
            res_temp.append(word)

        #return ' '.join([j[0] for j in [i.split('_') for i in text.split()] if j[1] not in stop_pos])
        return ' '.join(res_temp)
    except:
        traceback.print_exc()
        return None

# 单字剔除
def del_single(text):
    try:
        return ' '.join([i.strip() for i in text.split() if len(i.strip()) > 1])
    except:
        traceback.print_exc()
        return None

    
# 停用词剔除

f = codecs.open(main_path + 'Elara/Documents/paper/5_0_filter/stopword/stop_word.txt',encoding='utf-8')
stop_word = [i.strip() for i in f.readlines()]
f.close()
stop_word = stop_word + ['9:','1:','5:','A:',':00，','3:',':00-','业B:',':召','13:',':00；通过互联'] + ['本周','周末','上周','下周'] + [''.join(i) for i in list(itertools.product(['周','礼拜','星期'],['1','2','3','4','5','6','7','一','二','三','四','五','六','七','天']))]

def del_stop(text):
    try:
        return ' '.join([i for i in text.split() if i not in stop_word])
    except:
        traceback.print_exc()
        return 

# 日期词剔除

stop_time_word = [''.join(i) for i in list(itertools.product(['1','2','3','4','5','6','7','8','9','0'],['年','月','日']))] + [''.join(i) for i in list(itertools.product(['一','二','三','四','五','六','七','八','九','零'],['年','月','日']))]

def del_time(text):
    try:
        return ' '.join([i for i in text.split() if i[-2:] not in stop_time_word])
    except:
        traceback.print_exc()
        return 


    
# 词语清洗
def clear_word(text):
    return del_time(del_stop(del_single(del_pos(text))))


def clear_record(record):
    try:
        if len(record[4]) == 0 or record[4] == 0 or len(record[1]) == 0 or record[1] == 0:
            print(record[0],record[1],' title or content lenght is 0')
            return record
    except:
        if record[4] == None or record[1] == None:
            print(record[0],record[1],' title or content is None')
            return record
    try:
        title = clear_word(record[1])
        content = clear_word(record[4])
        if title == None:
            print(record[0],record[1],' title clear return None')
        if content == None:
            print(record[0],record[1],' content clear return None')
        record[1] = title
        record[4] = content
        return record 
    except:
        print(record[0],record[1],' error')
        return record

 
if __name__ ==  '__main__':
    
#i = os.listdir(main_path+'Elara/Documents/paper/seg/pos_unfilter/')[1]
    for i in os.listdir(main_path+'Elara/Documents/paper/4_seg/pos_unfilter/'):
        print('loading',i)
        temp = pd.read_csv(main_path+"Elara/Documents/paper/4_seg/pos_unfilter/"+i,
                               names=['name','title','url','date','content','cate','source','conments','uv','url_ch'], 
                               engine='python',
                               encoding="utf-8")
        temp2 = [[j for j in temp.iloc[i,:]] for i in range(len(temp))]
        print('load',i,' done')
        pre_cnt = [[i[0],i[1],len(i[4].split())] for i in temp2]
        
        print('clear_record ',i)
        pool = multiprocessing.Pool(processes = cores)
        res = pool.map(clear_record, temp2)
        pool.close()
        print('clear_record ',i,' done')
        post_cnt = [[i[0],i[1],len(i[4].split())] for i in res]
        
        
        result = pd.DataFrame(res)
        result.to_csv(main_path + 'Elara/Documents/paper/5_0_filter/corpus/' + i,encoding='utf-8',header=False,index=False)
        
        pre_cnt = pd.DataFrame(pre_cnt)
        pre_cnt.to_csv(main_path + 'Elara/Documents/paper/5_0_filter/cnt/pre_cnt' + i,encoding='utf-8',header=False,index=False)
        
        post_cnt = pd.DataFrame(post_cnt)
        post_cnt.to_csv(main_path + 'Elara/Documents/paper/5_0_filter/cnt/post_cnt' + i,encoding='utf-8',header=False,index=False)
        print('save',i,' done')
        
    

