# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 11:15:03 2017

@author: elara
"""

import platform
import pandas as pd
from pandasql import sqldf 
import multiprocessing
import copy
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
# 共196459记录
    

#q01 = 'select count(1) from merge_corpus where content is null'
#pysqldf(q01)
## 20875个content 空
#q02 = 'select count(1) from merge_corpus where title is null and content is null'
#pysqldf(q02)
## 20875个content和title空，说明2个一样多
#q03 = 'select count(1) from merge_corpus where title is null and content is null and url is not null'
#pysqldf(q03)
## 0没有真正的空文本


# 全局计算每日，每个词的tf df
## 按日期、url去重,去除空文本

#max(news_len)
#最大长度9841
#min(news_len)
#最小长度2
#np.mean(news_len)
#平均长度443.05
#np.median(news_len)
#中位长度323.0

def merge_dict_y2x(x,y):
    # 输入2个dict
    for k, v in y.items():
        if k in x.keys():
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

def decode_tf(kv_text):
    # 输入字符串 k:v
    d= {}
    if kv_text == None:
        print('decode_tf: input kv_text is null')
        return d
    else:
        for i in kv_text.split():
            d[i.split(':')[0]] = int(i.split(':')[1])
    return d

def decode_df(kv_text):
    # 输入字符串 k:v
    d= {}
    if kv_text == None:
        print('decode_df: input kv_text is null')
        return d
    else:
        for i in kv_text.split():
            d[i.split(':')[0]] = 1
    return d

def tf_line(line):
    line = list(line)
    title = decode_tf(line[0])
    content = decode_tf(line[1])
    return [title,content]

def df_line(line):
    line = list(line)
    title = decode_df(line[0])
    content = decode_df(line[1])
    return [title,content]


def day_tf(df):
    df = copy.deepcopy(df)
    if len(df.columns)==3:
        date = df['date'].iloc[0]
        res = df.loc[:,['title','content']].apply(tf_line,axis = 1)
        res_title = list(res['title'])
        res_content = list(res['content'])
        title = merge_dict([i for i in res_title])
        title = ' '.join([':'.join([i,str(title[i])]) for i in title])
        content = merge_dict([i for i in res_content])
        content = ' '.join([':'.join([i,str(content[i])]) for i in content])
        return [date,title,content]
    if len(df.columns)==4:
        name = df['name'].iloc[0]
        date = df['date'].iloc[0]
        res = df.loc[:,['title','content']].apply(tf_line,axis = 1)
        res_title = list(res['title'])
        res_content = list(res['content'])
        title = merge_dict([i for i in res_title])
        title = ' '.join([':'.join([i,str(title[i])]) for i in title])
        content = merge_dict([i for i in res_content])
        content = ' '.join([':'.join([i,str(content[i])]) for i in content])
        return [date,name,title,content]

def day_df(df):
    df = copy.deepcopy(df)
    if len(df.columns)==3:
        date = df['date'].iloc[0]
        res = df.loc[:,['title','content']].apply(df_line,axis = 1)
        res_title = list(res['title'])
        res_content = list(res['content'])
        title = merge_dict([i for i in res_title])
        title = ' '.join([':'.join([i,str(title[i])]) for i in title])
        content = merge_dict([i for i in res_content])
        content = ' '.join([':'.join([i,str(content[i])]) for i in content])
        return [date,title,content]
    if len(df.columns)==4:
        name = df['name'].iloc[0]
        date = df['date'].iloc[0]
        res = df.loc[:,['title','content']].apply(df_line,axis = 1)
        res_title = list(res['title'])
        res_content = list(res['content'])
        title = merge_dict([i for i in res_title])
        title = ' '.join([':'.join([i,str(title[i])]) for i in title])
        content = merge_dict([i for i in res_content])
        content = ' '.join([':'.join([i,str(content[i])]) for i in content])
        return [date,name,title,content]
    
def day_tfdf(dataf):
    tf = day_tf(dataf)
    df = day_df(dataf)
    if len(dataf.columns) == 3:
        date = dataf['date'].iloc[0]
        return pd.DataFrame({'date':[date],'title_tf':[tf[1]],'content_tf':[tf[2]],'title_df':[df[1]],'content_df':[df[2]]})
    if len(dataf.columns) == 4:
        date = dataf['date'].iloc[0]
        name = dataf['name'].iloc[0]
        return pd.DataFrame({'date':[date],'name':[name],'title_tf':[tf[2]],'content_tf':[tf[3]],'title_df':[df[2]],'content_df':[df[3]]})
        
def applyParallel(dfGrouped, func):
    with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
        ret_list = p.map(func, [group for name, group in dfGrouped])
    return pd.concat(ret_list)


if __name__ == '__main__':
    
    q1 = 'select distinct date,url,title,content,cate,source,conments,uv,url_ch from merge_corpus where content is not null and title is not null'
    print('running: \n',q1)
    unique_corpus = pysqldf(q1)
    print('nrow = ',len(unique_corpus))
	
    print('checking')
    for i in range(len(unique_corpus)):
        for j in unique_corpus.iloc[i,3].split():
            try:
                x = int(j.split(':')[1])
            except:
                print(j)
    
    print('saving overall news_len')
    news_len = [sum([int(j.split(':')[1]) for j in unique_corpus.iloc[i,3].split()]) for i in range(len(unique_corpus))] #直方图用R画
    pd.DataFrame(news_len).to_csv(main_path + 'Elara/Documents/paper/tiidf/news_len_overall.csv')
    
    
    print('start grouping')
    dftf_overall = applyParallel(unique_corpus.loc[:,['date','title','content']].groupby(['date']), day_tfdf)
    dftf_overall = dftf_overall[['date','title_tf','title_df','content_tf','content_df']]
    print('overall done')
    dftf_overall.to_csv(main_path+'Elara/Documents/paper/tiidf/'+'dftf_overall.csv',encoding='utf-8',header=False,index=False)
    
    
    
    
    q2 = 'select distinct name, date,url,title,content,cate,source,conments,uv,url_ch from merge_corpus where content is not null and title is not null'
    print('running: \n',q2)
    unique_name_corpus = pysqldf(q2)
    print('nrow = ',len(unique_name_corpus))
	
    print('start grouping')
    dftf_stock = applyParallel(unique_name_corpus.loc[:,['date','name' ,'title','content']].groupby(['date','name']), day_tfdf)
    dftf_stock = dftf_stock[['date', 'name','title_tf','title_df','content_tf','content_df']]
    print('stock done')
    dftf_stock.to_csv(main_path+'Elara/Documents/paper/tiidf/'+'dftf_stock.csv',encoding='utf-8',header=False,index=False)
                
    
    