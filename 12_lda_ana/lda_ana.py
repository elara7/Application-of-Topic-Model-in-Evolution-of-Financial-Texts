# -*- coding: utf-8 -*-
"""
Created on Fri Dec 22 13:04:01 2017

@author: elara
"""

import platform
import pandas as pd
from pandasql import sqldf 
import multiprocessing
import copy
import gensim
import numpy as np
import os
import logging
from datetime import datetime
import itertools
import copy
import re
import scipy
from scipy import stats
cores = multiprocessing.cpu_count()
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
    main_path2 = '/mnt/D/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    main_path2 = 'D:/'



tfidf_filter_corpus_path = main_path+ 'Elara/Documents/paper/9_0_tiidf/tfidf_filter/filterresult/'
lda_best_model_path = main_path2 + 'lda_run/bestmodel1/'
dict_path = main_path2 + 'lda_run/dic1/'

def find_rep_topic_no(d):
    d = copy.deepcopy(d)
    for x in range(d.shape[0]):
        for y in range(x+1,d.shape[1]):
            if d[y,x] > 1e-5 or np.isnan(d[y,x]):
                continue
            else:
                #print(y,x)
                d[y,:] = None
                d[:,y] = None
                #print('drop ',y)
    stop_topic2 = []
    for i in range(d.shape[0]):
        if np.isnan(d[i,0]):
            stop_topic2.append(i)
    return stop_topic2

def kld(p,q):
    if len(p)!=len(q):
        print('len p != len q')
        return None
    else:
        kl = 0
        for i in range(len(p)):
            kl += p[i]*(np.log(p[i])-np.log(q[i]))
    return kl

def jsd(p,q):
    if len(p)!=len(q):
        print('len p != len q')
        return None
    else:
        m=[]
        for i in range(len(p)):
            m.append(0.5*p[i] + 0.5*q[i])
        return 0.5 * kld(p,m) + 0.5* kld(q,m)
            
            
def cal_grab_score(topic):
    uni_topic = [1/len(topic)]*len(topic)
    if max(topic) >= 0.01 :
        return jsd(topic,uni_topic)
    else:
        return -10000

def cal_f1_score(a,b,w):
    return (w*w+1)*a*b/(w*w*a+b)

def cal_w_score(a,b,w):
    return a*w + b*(1-w)

    
#1.文件读取，按name归类，按stage排序
lda_name_list = os.listdir(lda_best_model_path)
model_name_list = list(set([i.split('.')[0] for i in lda_name_list]))
model_info = [[i.split('_')[0],i.split('_')[1],i.split('_')[2],i.split('_')[3]] for i in model_name_list]
stock_names = list(set([i[0] for i in model_info]))
stock_name = '上汽集团'
stage_list = sorted([int(i[1]) for i in model_info if i[0] == stock_name])

text_info = pd.read_csv(tfidf_filter_corpus_path + stock_name+'.csv',encoding='utf-8',engine='python',names = ['name','date','stage','content'])

#2.对于一个name，一个stage，做重复话题删除，定义话题质量分（和辣鸡话题的距离），话题活跃度分（文档概率求和），用F1均值得到话题得分排序
## load

def get_topic_score(model_info,text,dictionary,stage=0,stock_name='上汽集团',word_top_n=None,tfidf_top_n=10):
    name_stop = [stock_name,stock_name[0:2],stock_name[2:4]]
    topic_num = int([i[3] for i in model_info if i[0]==stock_name and i[1] ==str(stage)][0])
    model_name = ['_'.join(i) for i in model_info if i[0]==stock_name and i[1] == str(stage)][0]
    model_fit = gensim.models.ldamodel.LdaModel.load(lda_best_model_path + model_name)
    model_fit.id2word = dictionary
    ## 删除重复
    d = model_fit.diff(model_fit,distance = 'jensen_shannon')[0]
    stop_topics = find_rep_topic_no(d)
    
    ## 话题质量分
    grab = []
    for i in range(topic_num):
        if i in stop_topics:
            grab.append(-10000)
            continue
        ttt =[i[1] for i in model_fit.get_topic_terms(topicid=i,topn=len(dictionary))]
        grab.append(cal_grab_score(ttt))
    grab = [[i,grab[i]] for i in range(len(grab))]
    grab_score = grab


    
    ## 话题活跃度分

    texts = [text.iloc[i,3].split() for i in range(len(text)) if type(text.iloc[i,3])==str]
    corpus = [dictionary.doc2bow(text) for text in texts]
    active = list(pd.DataFrame([[j[1] for j in i] for i in list(model_fit.get_document_topics(corpus, minimum_probability=0, minimum_phi_value=0))]).apply(np.sum,axis = 0))
    active_score = [[i,active[i]/max(active)] for i in range(len(active))]
    
    # 话题相干分
    #cm = gensim.models.coherencemodel.CoherenceModel(model=model_fit, texts=texts, dictionary=dictionary, coherence='c_v')
    #cm_list = cm.get_coherence_per_topic()
    
    ## 话题区分度
   
    topic_term_matrix = model_fit.get_topics()
    topic_term_string = []
    for i in range(topic_num):
        topic_term = topic_term_matrix[i,:]
        temp_topic_term = []
        for j in range(len(topic_term)):
            if topic_term[j] < 0.001 :
                continue
            temp_topic_term.append([dictionary[j],topic_term[j]])
        temp_topic_term = pd.DataFrame(temp_topic_term)
        temp_topic_term.columns = ['word','freq']
        temp_topic_term = temp_topic_term.sort_values('freq',ascending=False)
        temp_topic_term.index = range(len(temp_topic_term))
        topic_term_string.append(' '.join([':'.join([temp_topic_term.iloc[k,0],str(temp_topic_term.iloc[k,1])]) for k in range(len(temp_topic_term))]))
        

    topic_word_fre = [[j.split(':') for j in i.split()] for i in topic_term_string]
    
    topic_word = [[j[0] for j in i] for i in topic_word_fre]
    word_df = {}
    for i in range(topic_num):
        if i in stop_topics: #不重复计算
            continue
        for j in topic_word[i]:
            if j in word_df.keys():
                word_df[j] += 1
            else:
                word_df[j] =1
                
    #high_f_line = np.percentile(list(word_f.values()),95)
    #high_f_line = np.percentile(list(word_f.values()),95)
    
    high_f_word = [i for i in word_df.keys() if word_df[i]/(topic_num-len(stop_topics))>0.1] + [stock_name]
    high_f_word_time=[]
    for i in topic_word:
        cnt = 0
        if len(i)==0:
            high_f_word_time.append(0)
            continue
        for j in i:
            if j in high_f_word:
                cnt+=1
        high_f_word_time.append((len(i)-cnt)/len(i))
    dist_score = []
    for i in range(topic_num):
        dist_score.append([i,high_f_word_time[i]/max(high_f_word_time)])
    
    ## 原始结果
    word = []
    for i in topic_word_fre:
        temp_word = []
        word_cnt = 0
        for j in i:
            if word_top_n!=None:
                if word_cnt>=word_top_n:
                    break
            if re.match(r'(快速发展)|(公司)|(亿.)|(万.)|(股市场)|(的竞争)|(.*个股)|(领域)|(的.*)',j[0]) == None and re.match('|'.join(['(.*'+i+'.*)' for i in name_stop]),j[0]) == None:
                temp_word.append(j)
                word_cnt+=1
        word.append(' '.join([':'.join([x[0],str(x[1])]) for x in temp_word]))
            
            
    # tfidf
    word_tfidf = []
    for ind in range(topic_num):
        if ind in stop_topics or len(topic_word_fre[ind])==0:
            word_tfidf.append('')
            continue
        i = topic_word_fre[ind]
        temp_tfidf = []
        t=0
        for j in i:
            if tfidf_top_n!=None:
                if t>=tfidf_top_n:
                    break
            if re.match(r'(快速发展)|(公司)|(亿.)|(万.)|(股市场)|(的竞争)|(.*个股)|(领域)|(的.*)',j[0]) == None and re.match('|'.join(['(.*'+i+'.*)' for i in name_stop]),j[0]) == None :
                temp_tfidf.append([j[0],float(j[1])*np.log((topic_num-len(stop_topics))/word_df[j[0]])])
                t+=1
            
        if len(temp_tfidf)==0:
            word_tfidf.append('')
            continue
        temp_tfidf = pd.DataFrame(temp_tfidf)
        temp_tfidf.columns = ['word','tfidf']
        temp_tfidf = temp_tfidf.sort_values('tfidf',ascending=False)
        temp_tfidf = [list(temp_tfidf.iloc[x,:]) for x in range(len(temp_tfidf))]
        word_tfidf.append(' '.join([':'.join([x[0],str(x[1])]) for x in temp_tfidf]))
    
    topic_score = [[i,grab_score[i][1],active_score[i][1],dist_score[i][1],word_tfidf[i],word[i]] for i in range(topic_num) if grab_score[i][1]>0.1 and active_score[i][1]>0 and dist_score[i][1]>0 and len(word_tfidf[i].split())>5 and i not in stop_topics]
    
    topic_info = pd.DataFrame(topic_score)
    topic_info.columns = ['topic_id','grab_score','active_score','dist_score','word_tfidf','word_raw']

    return topic_info


#3.按顺序对相邻stage计算排名前十的话题，两两之间重复度 = 前十个词之间重复数/10，完全重复定义为持续，重复789个定义为关联，重复56个定义为相关




stage = stage_list[0]
temp = text_info.loc[text_info['stage']==stage]
text = temp.sort_values(by=['date','content'])
dictionary = gensim.corpora.Dictionary.load(dict_path + stock_name + str(stage))
topic_info1 = get_topic_score(model_info,text,dictionary,stage,stock_name,10)
topic_info11 = get_topic_score(model_info,text,dictionary,stage,stock_name)

stage = stage_list[1]
temp = text_info.loc[text_info['stage']==stage]
texts = temp.sort_values(by=['date','content'])
dictionary = gensim.corpora.Dictionary.load(dict_path + stock_name + str(stage))
topic_info2 = get_topic_score(model_info,texts,dictionary,stage,stock_name,10)
topic_info21 = get_topic_score(model_info,text,dictionary,stage,stock_name)

stage = stage_list[2]
temp = text_info.loc[text_info['stage']==stage]
texts = temp.sort_values(by=['date','url'])
dictionary = gensim.corpora.Dictionary.load(dict_path + stock_name + str(stage))
topic_info3 = get_topic_score(model_info,texts,dictionary,stage,stock_name,10)
topic_info31 = get_topic_score(model_info,texts,dictionary,stage,stock_name)

stage = stage_list[3]
temp = text_info.loc[text_info['stage']==stage]
texts = temp.sort_values(by=['date','url'])
dictionary = gensim.corpora.Dictionary.load(dict_path + stock_name + str(stage))
topic_info4 = get_topic_score(model_info,texts,dictionary,stage,stock_name,10)
topic_info41 = get_topic_score(model_info,texts,dictionary,stage,stock_name)



#4.对持续话题，画活跃度分随阶段变化图

#5.对所有话题，画关联网络
#def spearman_topic(topic1,topic2):
#    topic = {}
#    l1=0
#    for i in topic1.split():
#        if l1>=50:
#            break
#        word,prob = i.split(':')
#        topic[word]=[float(prob)]
#        l1+=1
#    l2=0
#    for j in topic2.split():
#        if l2>=50:
#            break
#        word,prob = j.split(':')
#        l2+=1
#        if word in topic.keys():
#            topic[word].append(float(prob))
#        else:
#            continue
#    del_key = [k for k in topic.keys() if len(topic[k])<2]
#    for d in del_key:
#        del topic[d]
#    if len(topic) < 20:
#    #min([100,0.5*min([l1,l2])]):
#        return [0,-1]
#    else:
#        topic1 = [i[0] for i in list(topic.values())]
#        topic2 = [i[1] for i in list(topic.values())]
#        res = scipy.stats.spearmanr(topic1,topic2)
#        return [res[0],res[1]]

def w_order_cor(dict_ab):
    n = len(dict_ab)
    d = sum([pow( ( dict_ab[i][0] - dict_ab[i][1] ) , 2 ) * ( 2 * n + 2 - dict_ab[i][0] - dict_ab[i][1] ) for i in dict_ab.keys()])
    rw = 1 - ( 6 * d / ( pow(n,4) + pow(n,3) - pow(n,2) - n ) )
    var = ( 31 * pow(n,2) + 60 * n + 26 ) / ( 30 * ( pow(n,3) + pow(n,2) - n - 1 ) )
    srw = rw/var
    p = 2 * ( 1 - scipy.stats.norm.cdf(srw,0,1) )
    return [rw,p]
    
def spearman_topic(topic1,topic2):
    nn=10
    first_word1 = topic1.split()[0][0]
    first_word2 = topic2.split()[0][0]
    if first_word1!=first_word2:
        return [-1,-1]
    topic = {}
    l1=0
    for i in topic1.split():
        if l1>=nn:
            break
        word,prob = i.split(':')
        if float(prob)<0.001:
            l1+=1
            continue
        topic[word]=[float(prob),None]
        l1+=1
    l2=0
    for j in topic2.split():
        if l2>=nn:
            break
        word,prob = j.split(':')
        if float(prob)<0.001:
            l2+=1
            continue
        l2+=1
        if word in topic.keys():
            topic[word][1] = (float(prob))
        else:
            topic[word] = [None,float(prob)]
    
    for i in topic.keys():
        if topic[i][0] == None:
            topic[i][0] = 0
        if topic[i][1] == None:
            topic[i][1] = 0
    

    topic1 = [i[0] for i in list(topic.values())]
    topic2 = [i[1] for i in list(topic.values())]
    res = scipy.stats.weightedtau(topic1,topic2,rank = True, weigher=None,additive=True)
    return [res[0],res[1]]
    
spearman_res_table = []
for i in range(len(topic_info1)):
    for j in range(i+1,len(topic_info2)):
        topic1_id = topic_info11['topic_id'].iloc[i]
        topic1_str = topic_info11['word_raw'].iloc[i]
        topic2_id = topic_info21['topic_id'].iloc[j]
        topic2_str = topic_info21['word_raw'].iloc[j]
        spearman_res = spearman_topic(topic1_str,topic2_str)
        if spearman_res[1]==-1 or spearman_res[0]<0:
            continue
        else:
            spearman_res_table.append([topic1_id,topic2_id]+spearman_res)
spearman_res_table=pd.DataFrame(spearman_res_table)



topic1 = topic_info11['word_raw'].iloc[16]
topic2 = topic_info21['word_raw'].iloc[49]
spearman_topic(topic1,topic2)

name_stop

re.match('|'.join(['(.*'+i+'.*)' for i in name_stop]),'dd') == None 
