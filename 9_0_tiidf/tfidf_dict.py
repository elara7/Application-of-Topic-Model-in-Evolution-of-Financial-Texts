# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 22:05:18 2017

@author: elara
"""


import platform
import pandas as pd
import multiprocessing
import pickle
cores = multiprocessing.cpu_count()*2
print('set cores = ',cores)


if platform.system() == 'Linux':
    main_path = '/mnt/c/'
    #main_path = '/media/elara/56E8C0A9E8C088A7/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
dftf_overall_path = main_path+ 'Elara/Documents/paper/tiidf/dftf_overall.csv'
dftf_stock_path = main_path+ 'Elara/Documents/paper/tiidf/dftf_stock.csv'

dftf_overall = pd.read_csv(dftf_overall_path,names = ['date','title_tf','title_df','content_tf','content_df'])
dftf_stock = pd.read_csv(dftf_stock_path,names = ['date', 'name','title_tf','title_df','content_tf','content_df'])
print('load dftf data')

def merge_dict_y2x(x,y):
    # 输入2个dict
    for k, v in y.items():
        if k in x.keys():
            print(k,' already in x')
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

def gen_dict(t_tf,t_df,c_tf,c_df):
    t_tf_dict = decode_tfdf(t_tf)
    t_df_dict = decode_tfdf(t_df)
    c_tf_dict = decode_tfdf(c_tf)
    c_df_dict = decode_tfdf(c_df)
    d = {'t_tf':t_tf_dict, 't_df':t_df_dict, 'c_tf':c_tf_dict, 'c_df':c_df_dict}
    return d

def gen_time_dict(date,t_tf,t_df,c_tf,c_df):
    dftf = {}
    dftf[date] = gen_dict(t_tf,t_df,c_tf,c_df)
    return dftf

def get_time_dict(record):
    date = record['date']
    t_tf = record['title_tf']
    t_df = record['title_df']
    c_tf = record['content_tf']
    c_df = record['content_df']
    return gen_time_dict(date,t_tf,t_df,c_tf,c_df)

def get_overall_dict(i):
    record = dftf_overall.iloc[i,:]
    return get_time_dict(record)

def get_stock_dict(i):
    record = dftf_stock.iloc[i,:]
    return get_time_dict(record)

def get_overall_dict_all():
    pool = multiprocessing.Pool(processes = cores)
    overall_result = pool.map(get_overall_dict,range(len(dftf_overall)))
    pool.close()
    return merge_dict(overall_result)

def get_stock_dict_all():
    pool = multiprocessing.Pool(processes = cores)
    stockname = list(set(dftf_stock['name']))
    stock_dict={}
    for i in stockname:
        print('getting dict of ',i)
        index_list = dftf_stock[dftf_stock['name']==i].index.tolist()
        overall_result = pool.map(get_stock_dict,index_list)
        try:
            temp = stock_dict[i]
            print(i,' is already in dict')
        except:
            pass
        stock_dict[i] = merge_dict(overall_result)
    pool.close()		
    return stock_dict

def get_stock_dict_stream():
    pool = multiprocessing.Pool(processes = cores)
    stockname = list(set(dftf_stock['name']))
    for i in stockname:
        print('getting dict of ',i)
        index_list = dftf_stock[dftf_stock['name']==i].index.tolist()
        overall_result = pool.map(get_stock_dict,index_list)
        res = merge_dict(overall_result)
        f1 = open(main_path+ 'Elara/Documents/paper/tiidf/dict/stock/'+ i +'_dftf_dict.txt',"wb")
        pickle.dump(res, f1)
        f1.close()
    pool.close()		
    return None
	
print('generating over_all_tfdf')
over_all_tfdf = get_overall_dict_all()
print('generating over_all_tfdf done! saving...')
f1 = open(main_path+ 'Elara/Documents/paper/tiidf/dict/overall/dftf_overall_dict.txt',"wb")
pickle.dump(over_all_tfdf, f1)
f1.close()
print('save over_all_tfdf done!')

# print('generating stock_tfdf')
# stock_tfdf = get_stock_dict_all()
# print('generating stock_tfdf done! saving...')
# f1 = open(main_path+ 'Elara/Documents/paper/tiidf/dftf_stock_dict.txt',"wb")
# pickle.dump(stock_tfdf, f1)
# f1.close()
# print('save stock_tfdf done!')

print('generating stock_tfdf')
get_stock_dict_stream()
print('save stock_tfdf done!')

