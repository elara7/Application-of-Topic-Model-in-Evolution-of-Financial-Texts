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
    name_temp = file_name.split('.')[0].split('_')
    stock_name = name_temp[0]
    start_stage = name_temp[1]
    end_stage = name_temp[2]
    cor_info = pd.read_csv(track_path + file_name,engine='python',encoding='utf-8',
                           names = ['sim','none','leftid','leftactive','lefttfidf','leftstr','rightid','rightactive','righttfidf','rightstr'],header=None)
    topic_info_s =  pd.read_csv(info_path + stock_name+'_'+start_stage+'.csv',engine='python',encoding='utf-8',
                           names = ['topic_id','grab_score','active_score','word_tfidf','word_raw'],header=None)
    topic_info_s = topic_info_s.sort_values(by='active_score',ascending=False)
    topic_info_s.index = range(len(topic_info_s))
    topic_info_e =  pd.read_csv(info_path + stock_name+'_'+end_stage+'.csv',engine='python',encoding='utf-8',
                           names = ['topic_id','grab_score','active_score','word_tfidf','word_raw'],header=None)
    topic_info_e = topic_info_e.sort_values(by='active_score',ascending=False)
    topic_info_e.index = range(len(topic_info_e))
    cor_info = cor_info.sort_values(by='leftactive',ascending=False)
    cor_info.index = range(len(cor_info))
    name_dict = {' ':0,'  ':1,'   ':2,'    ':3}
    index=3
    res = [[0,1,0.0000001],[1,2,0.0000001],[2,3,0.0000001]]
    cnt=0
    for i in range(len(topic_info_s)):
        if cnt >= 10:
            break
        left_main_word = topic_info_s['word_raw'][i].split()[0].split(':')[0]
        left_sub_word = ' '.join([i.split(':')[0] for i in topic_info_s['word_raw'][i].split()[1:5]])
        left_active = topic_info_s['active_score'][i]
        if 'l'+left_main_word in name_dict.keys():
            left_main_word = name_dict['l'+left_main_word]
        else:
            index+=1
            name_dict['l'+left_main_word] = index
            left_main_word = name_dict['l'+left_main_word]
        if 'l'+left_sub_word in name_dict.keys():
            left_sub_word = name_dict['l'+left_sub_word]
        else:
            index+=1
            name_dict['l'+left_sub_word] = index
            left_sub_word = name_dict['l'+left_sub_word]
        res.append([left_main_word,left_sub_word,left_active])
        cnt+=1
      
    cnt=0
    for i in range(len(topic_info_e)):
        if cnt >= 10:
            break
        right_main_word = topic_info_e['word_raw'][i].split()[0].split(':')[0]
        right_sub_wrod = ' '.join([i.split(':')[0] for i in topic_info_e['word_raw'][i].split()[1:5]])
        right_active = topic_info_e['active_score'][i]
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
        res.append([right_sub_wrod,right_main_word,right_active])
        cnt+=1

    cnt1=0
    cnt2=0
    flag=0
    for a in range(len(topic_info_s)):
        print(cnt1,cnt2)
        if cnt1>=10 :
            break
        cnt2=0
        for b in range(len(topic_info_e)):
            if cnt2>=10:
                break
            left_main_word_raw = topic_info_s['word_raw'][a].split()[0].split(':')[0]
            left_sub_word_raw = ' '.join([i.split(':')[0] for i in topic_info_s['word_raw'][a].split()[1:5]])
            right_main_word_raw = topic_info_e['word_raw'][b].split()[0].split(':')[0]
            right_sub_wrod_raw = ' '.join([i.split(':')[0] for i in topic_info_e['word_raw'][b].split()[1:5]])
            
            if left_main_word_raw==right_main_word_raw:
                main_cor=1
            else:
                main_cor=0
            
            for i in range(len(cor_info)):
                
                left_main_word = cor_info['leftstr'][i].split()[0].split(':')[0]
                left_sub_wrod = ' '.join([i.split(':')[0] for i in cor_info['leftstr'][i].split()[1:5]])
                
                right_main_word = cor_info['rightstr'][i].split()[0].split(':')[0]
                right_sub_wrod = ' '.join([i.split(':')[0] for i in cor_info['rightstr'][i].split()[1:5]])
                
                if left_main_word_raw==left_main_word and left_sub_word_raw==left_sub_wrod and right_main_word_raw==right_main_word and right_sub_wrod_raw==right_sub_wrod:
                    
                    left_active = cor_info['leftactive'][i]
                    right_active = cor_info['rightactive'][i]
                    
                    lr_cor = cor_info['sim'][i]
                
                    
                    if lr_cor <0.0001:
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
                    res.append([left_sub_wrod,right_sub_wrod,max(main_cor*lr_cor,0.0000001)])
                    flag=1
                else:
                    continue
            if flag==0:
                res.append([name_dict['l'+left_sub_word_raw],name_dict['r'+right_sub_wrod_raw],max(main_cor*0.0001,0.0000001)])
            else:
                flag=0
            cnt2+=1
        cnt1+=1
    
    res = pd.DataFrame(res)
    res = res.drop_duplicates()
    res.columns=['s','t','v']
    res.sort_values(by=['s','v','t'],ascending=False)
    namedf = [[i[1],i[0][1:]] for i in name_dict.items()]
    if len(namedf)<=0 or len(res)<=0:
        continue
    res.to_csv(main_path2 + '/lda_run/topic_plot_res1/'+file_name,header=None,index=None,encoding='gbk')
    pd.DataFrame(namedf).to_csv(main_path2 + '/lda_run/topic_plot_name1/'+file_name,header=None,index=None,encoding='gbk')
