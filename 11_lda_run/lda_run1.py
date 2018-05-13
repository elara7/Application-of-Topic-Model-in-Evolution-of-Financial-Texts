# -*- coding: utf-8 -*-
"""
Created on Thu Dec 21 01:09:20 2017

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
cores = multiprocessing.cpu_count()
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
    main_path2 = '/mnt/D/'
if platform.system() == 'Windows':
    main_path = 'D:/paper_c/'
    main_path2 = 'D:/ldamodel20180417/'

passes = 20
iterations = 500

tfidf_filter_corpus_path = main_path+ 'Elara/Documents/paper/9_0_tiidf/tfidf_filter/filterresult/'


def train_lda(name,stage,corpus,dictionary,numoftopic):
    try:
        doc_len = len(corpus)
        #print('fitting',name,stage,numoftopic)
        lda_fit = gensim.models.ldamodel.LdaModel(corpus=corpus, num_topics=numoftopic, id2word=dictionary, 
                                              distributed=False, chunksize=2000, passes=passes, 
                                              alpha='auto', eta='auto', decay=0.5, offset=1.0, eval_every=0, iterations=iterations, 
                                              gamma_threshold=0.001, minimum_probability=0, 
                                              random_state=777, ns_conf=None, minimum_phi_value=0)
        #print('saving',name,stage,numoftopic)
        lda_fit.save(main_path2 + 'lda_run/resultmodel1/'+name+'_'+str(stage)+'_'+str(doc_len)+'_'+str(numoftopic) ,ignore=('state', 'dispatcher','id2word'))
        return lda_fit
    except:
        print(name,stage,numoftopic,'fit failed')

def find_reptopic(lda_fit_model):
    is_rep = 0
    d = lda_fit_model.diff(lda_fit_model)[0]
    for x in range(len(d)):
        for y in range(x+1,len(d)):
            if d[y,x]==0:
                is_rep=1
                break
    return is_rep

def find_k(name,stage,corpus,dictionary,small,large):
    if large == None:
        numoftopic = small
    else:
        numoftopic = int((small + large)/2)
    #print(name,stage,' numoftopic = ',numoftopic)
    lda_fit_model = train_lda(name,stage,corpus,dictionary,numoftopic)
    is_rep = find_reptopic(lda_fit_model)
    
    if large == None:
        if is_rep:
            large = copy.deepcopy(small)
            small = int(small/2)
        else:
            large = copy.deepcopy(int(small*2))
    else:
        if is_rep:
            large = numoftopic
        else:
            small = numoftopic
    return [lda_fit_model,small,large,is_rep,numoftopic]

def find_best_k(name,stage,corpus,dictionary):
    doc_len = len(corpus)
    if doc_len <=10:
        numoftopic = 20
    elif doc_len <=500:
        numoftopic = 50
    elif doc_len <= 1500:
        numoftopic = 200
    else:
        numoftopic = 300
    small = 0
    while 1:
        #print(name,stage,' numoftopic = ',numoftopic)
        lda_fit_model = train_lda(name,stage,corpus,dictionary,numoftopic)
        is_rep = find_reptopic(lda_fit_model)
        if is_rep:
            large = numoftopic
            last_rep_model = lda_fit_model
            last_rep_numoftopic = numoftopic
            if small ==0:
                small =1
            break
        else:
            small = numoftopic
            numoftopic = 2*numoftopic

    
    for i in range(6):
        model,small,large,is_rep,numoftopic = find_k(name,stage,corpus,dictionary,small,large)
        if is_rep:
            last_rep_model = model
            last_rep_numoftopic = numoftopic
        if np.abs(small-large)<=1:
            break
    last_rep_model.save(main_path2 + 'lda_run/bestmodel1/'+name+'_'+str(stage)+'_'+str(doc_len)+'_'+str(last_rep_numoftopic) ,ignore=('state', 'dispatcher','id2word'))
    return None


        
def lda_run(stockname):
    print('loading ',stockname)
    text_info = pd.read_csv(tfidf_filter_corpus_path + stockname,encoding='utf-8',engine='python',names = ['name','date','stage','content'])
    stage_list = sorted(list(set(text_info['stage'])))
    for i in stage_list:
        
        stage = int(i)
        name = stockname.split('.')[0]
        print('fitting ',name,stage)
        temp = text_info.loc[text_info['stage']==i]
        temp = temp.sort_values(by=['date','content'])
        temp.index = range(len(temp))
        texts = [temp.iloc[i,3].split() for i in range(len(temp)) if type(temp.iloc[i,3])==str]
        
        try:
            dictionary = gensim.corpora.Dictionary(texts)
            corpus = [dictionary.doc2bow(text) for text in texts]
            dictionary.save(main_path2 + 'lda_run/dic1/'+name+str(stage))
        except:
            print(name,stage,'dict failed')
            continue
#        try:
#            find_best_k(name,stage,corpus,dictionary)
#        except:
#            print(name,stage,'find best k failed')
        find_best_k(name,stage,corpus,dictionary)
    print(stockname,'done')
    return None

if __name__ == '__main__':
    lda_run('上汽集团.csv')
