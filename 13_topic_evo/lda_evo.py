# -*- coding: utf-8 -*-
"""
Created on Mon Dec 25 09:34:05 2017

@author: elara
"""



import platform
import pandas as pd
from pandasql import sqldf 
import os
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
    main_path2 = '/mnt/d/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    main_path2 = 'D:/'

track_path = main_path2 + '/lda_run/topic_track_new/'
info_path = main_path2 + '/lda_run/topic_info_new/'

for file_name in os.listdir(track_path):
    cor_info = pd.read_csv(track_path + file_name,engine='python',encoding='utf-8',
                           names = ['sim','none','leftid','leftactive','lefttfidf','leftstr','rightid','rightactive','righttfidf','rightstr'],header=None)
    
    cor_info = cor_info.sort_values(by='leftactive',ascending=False)
    name_dict = {}
    index=-1
    res = []
    for i in range(len(cor_info)):
        
        left_main_word = cor_info['leftstr'][i].split()[0].split(':')[0]
        left_sub_wrod = ' '.join([i.split(':')[0] for i in cor_info['leftstr'][i].split()[1:5]])
        left_active = cor_info['leftactive'][i]
        
        right_main_word = cor_info['rightstr'][i].split()[0].split(':')[0]
        right_sub_wrod = ' '.join([i.split(':')[0] for i in cor_info['rightstr'][i].split()[1:5]])
        right_active = cor_info['rightactive'][i]
        
        lr_cor = cor_info['sim'][i]
        
        if lr_cor <0.1:
            continue
        
        if 'l'+left_main_word in name_dict.keys():
            left_main_word = name_dict['l'+left_main_word]
        else:
            index+=1
            name_dict['l'+left_main_word] = index
            left_main_word = name_dict['l'+left_main_word]
            
        if 'l'+left_sub_wrod in name_dict.keys():
            left_sub_wrod = name_dict['l'+left_sub_wrod]
        else:
            index+=1
            name_dict['l'+left_sub_wrod] = index
            left_sub_wrod = name_dict['l'+left_sub_wrod]
            
        if 'r'+right_main_word in name_dict.keys():
            right_main_word = name_dict['r'+right_main_word]
        else:
            index+=1
            name_dict['r'+right_main_word] = index
            right_main_word = name_dict['r'+right_main_word]
            
        if 'r'+right_sub_wrod in name_dict.keys():
            right_sub_wrod = name_dict['r'+right_sub_wrod]
        else:
            index+=1
            name_dict['r'+right_sub_wrod] = index
            right_sub_wrod = name_dict['r'+right_sub_wrod]
        
        res.append([left_main_word,left_sub_wrod,left_active])
        res.append([right_sub_wrod,right_main_word,right_active])
        res.append([left_sub_wrod,right_sub_wrod,lr_cor])
    
    namedf = [[i[1],i[0][1:]] for i in name_dict.items()]
    if len(namedf)<=0 or len(res)<=0:
        continue
    pd.DataFrame(res).to_csv(main_path2 + '/lda_run/topic_plot_res1/'+file_name,header=None,index=None,encoding='gbk')
    pd.DataFrame(namedf).to_csv(main_path2 + '/lda_run/topic_plot_name1/'+file_name,header=None,index=None,encoding='gbk')
