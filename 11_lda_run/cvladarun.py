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
    main_path = 'C:/'
    main_path2 = 'D:/'

passes = 20
iterations = 500

tfidf_filter_corpus_path = main_path+ 'Elara/Documents/paper/tiidf/tfidf_filter/filterresult/'


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
        lda_fit.save(main_path2 + 'lda_run/resultmodel_cv/'+name+'_'+str(stage)+'_'+str(doc_len)+'_'+str(numoftopic) ,ignore=('state', 'dispatcher','id2word'))
        return lda_fit
    except:
        print(name,stage,numoftopic,'fit failed')

def cal_cv(lda_fit_model,text,dictionary):
    try:
        cm = gensim.models.coherencemodel.CoherenceModel(model=lda_fit_model, texts=text, dictionary=dictionary, coherence='c_v',processes=1)
        gcm = cm.get_coherence()
    except:
        print('get cm failed')
        return None
    return gcm

def find_best_k(name,stage,corpus,dictionary,texts):
    num_of_topic = [5,10,15,20,25,50,75,100,125,150,175,200,225,250,275,300]
    doc_len=len(texts)
    best_model=[]
    cm_list =[]
    best_cm = []
    best_n = []
    for i in num_of_topic:
        try:
            lda_fit_model = train_lda(name,stage,corpus,dictionary,i)
            cm = cal_cv(lda_fit_model,texts,dictionary)
            if cm==None:
                continue
            if len(cm_list)==0:
                best_model = lda_fit_model
                best_cm = cm
                best_n = i
            else:
                if cm > best_cm:
                    best_model = lda_fit_model
                    best_cm = cm
                    best_n = i
            cm_list.append([i,cm])
        except:
            print(name,stage,'deal with cm failed')
            
    best_model.save(main_path2 + 'lda_run/bestmodel_cv/'+name+'_'+str(stage)+'_'+str(doc_len)+'_'+str(best_n) ,ignore=('state', 'dispatcher','id2word'))
    
    pd.DataFrame(cm_list).to_csv(main_path2 + 'lda_run/cv_value/'+name+'_'+str(stage)+'_'+str(doc_len)+'_'+str(best_n)+'.csv',encoding='UTF-8',header=None,index=None)
    return None


        
def lda_run(stockname):
    print('loading ',stockname)
    text_info = pd.read_csv(tfidf_filter_corpus_path + stockname,encoding='utf-8',engine='python',names = ['stage','name','date','title','url','content','ch','source','url_ch'])
    stage_list = sorted(list(set(text_info['stage'])))
    for i in stage_list:
        
        stage = i
        name = stockname.split('.')[0]
        print('fitting ',name,stage)
        temp = text_info.loc[text_info['stage']==i]
        temp = temp.sort_values(by=['date','url'])
        
        texts = [temp.iloc[i,5].split() for i in range(len(temp))]
        
        try:
            dictionary = gensim.corpora.Dictionary(texts)
            corpus = [dictionary.doc2bow(text) for text in texts]
            dictionary.save(main_path2 + 'lda_run/dic_cv/'+name+str(stage))
        except:
            print(name,stage,'dict failed')
            continue
        try:
            find_best_k(name,stage,corpus,dictionary,texts)
        except:
            print(name,stage,'find best k failed')
    print(stockname,'done')
    return None

if __name__ == '__main__':
    stocknamelist = os.listdir(tfidf_filter_corpus_path)
    stocknames = [stocknamelist[i] for i in range(len(stocknamelist)) if i%2==1]
    pool = multiprocessing.Pool(processes = cores)
    pool.map(lda_run,stocknames)
