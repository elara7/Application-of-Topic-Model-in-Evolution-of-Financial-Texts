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
import re
import scipy
import traceback
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

def get_topic_score(model_info,text,dictionary,stage=0,stock_name='上汽集团',word_top_n=None,tfidf_top_n=10):
    name_stop = [stock_name,stock_name[0:2],stock_name[2:4]]
    topic_num = int([i[3] for i in model_info if i[0]==stock_name and i[1] ==str(stage)][0])
    model_name = ['_'.join(i) for i in model_info if i[0]==stock_name and i[1] == str(stage)][0]
    model_fit = gensim.models.ldamodel.LdaModel.load(lda_best_model_path + model_name)
    model_fit.id2word = dictionary
    ## 删除重复
    d = model_fit.diff(model_fit,distance = 'jensen_shannon')[0]
    stop_topics = find_rep_topic_no(d)
    if len(stop_topics)==0:
        print('无重复')
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
#    dist_score = []
#    for i in range(topic_num):
#        dist_score.append([i,high_f_word_time[i]/max(high_f_word_time)])
    
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
        temp_tfidf.index = range(len(temp_tfidf))
        temp_tfidf = [list(temp_tfidf.iloc[x,:]) for x in range(len(temp_tfidf))]
        word_tfidf.append(' '.join([':'.join([x[0],str(x[1])]) for x in temp_tfidf]))
    
    topic_score = [[i,grab_score[i][1],active_score[i][1],word_tfidf[i],word[i]] for i in range(topic_num) if i not in stop_topics and grab_score[i][1]>0.1 and active_score[i][1]>0  and len(word_tfidf[i].split())>5]
    
    topic_info = pd.DataFrame(topic_score)
    topic_info.columns = ['topic_id','grab_score','active_score','word_tfidf','word_raw']

    return topic_info



def spearman_topic(topic1,topic2):
    nn=10
    first_word1 = topic1.split()[0].split(':')[0]
    first_word2 = topic2.split()[0].split(':')[0]
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



lda_name_list = os.listdir(lda_best_model_path)
model_name_list = list(set([i.split('.')[0] for i in lda_name_list]))
model_info = [[i.split('_')[0],i.split('_')[1],i.split('_')[2],i.split('_')[3]] for i in model_name_list]
stock_names = list(set([i[0] for i in model_info]))

def get_result(stock_name):
    stage_list = sorted([int(i[1]) for i in model_info if i[0] == stock_name])
    
    text_info = pd.read_csv(tfidf_filter_corpus_path + stock_name+'.csv',encoding='utf-8',engine='python',names = ['name','date','stage','content'])
    
    last_stage_info = []
    for stage in stage_list:
        try:
            temp = text_info.loc[text_info['stage']==stage]
            text = temp.sort_values(by=['date','content'])
            text.index = range(len(text))
            dictionary = gensim.corpora.Dictionary.load(dict_path + stock_name + str(stage))
            topic_info = get_topic_score(model_info,text,dictionary,stage,stock_name)
            topic_info[['topic_id','grab_score','active_score','word_raw']].to_csv(main_path2 + 'lda_run/topic_info_ppt/'+stock_name+'_'+str(stage)+'.csv',encoding='GBK',header=None,index=None)
            if len(topic_info)==0:
                print(stock_name,stage,'get 0 topic')
                continue
            if len(last_stage_info)==0:
                last_stage_info = [topic_info,stage]
                continue
            else:
                spearman_res_table = []
                for i in range(len(last_stage_info[0])):
                    for j in range(len(topic_info)):
                        topic1_id = last_stage_info[0]['topic_id'].iloc[i]
                        topic1_str = last_stage_info[0]['word_raw'].iloc[i]
                        topic2_id = topic_info['topic_id'].iloc[j]
                        topic2_str = topic_info['word_raw'].iloc[j]
                        spearman_res = spearman_topic(topic1_str,topic2_str)
                        if spearman_res[1]==-1 or spearman_res[0]<=0 :
                            continue
                        else:
                            spearman_res_table.append(spearman_res+[topic1_id,last_stage_info[0]['active_score'].iloc[i],topic1_str.split()[0],topic2_id,topic_info['active_score'].iloc[j],topic2_str.split()[0]])
                if len(spearman_res_table)==0:
                    last_stage_info = [topic_info,stage]
                    print(stage)
                    continue
                spearman_res_table=pd.DataFrame(spearman_res_table)
                spearman_res_table.to_csv(main_path2+'lda_run/topic_track_ppt/'+stock_name+'_'+str(last_stage_info[1])+'_'+str(stage)+'.csv',encoding='UTF-8',header=None,index=None)
                last_stage_info = [topic_info,stage]
        except:
            traceback.print_exc()
            print(stock_name,stage,'failed')


if __name__ == '__main__':
    get_result(stock_names[0])