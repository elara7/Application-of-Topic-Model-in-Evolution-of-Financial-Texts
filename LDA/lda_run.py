# -*- coding: utf-8 -*-
"""
Created on Tue Dec 19 15:34:28 2017

@author: elara
"""



import platform
import pandas as pd
from pandasql import sqldf 
import gensim
import os
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
tfidf_filter_corpus_path = main_path+ 'Elara/Documents/paper/tiidf/tfidf_filter/filterresult/'

for name in os.listdir(tfidf_filter_corpus_path):
    text_info = pd.read_csv(tfidf_filter_corpus_path + name,encoding='utf-8',engine='python',names = ['stage','name','date','title','url','content','ch','source','url_ch'])
    text_all = []
    stage_list = sorted(list(set(text_info['stage'])))
    for i in stage_list:
        temp = text_info.loc[text_info['stage']==i]
        text_all.append([list(temp.iloc[j,:]) for j in range(len(temp))])
        
    texts = [i[5].split() for i in text_all[0]]
    # 向量化
    dictionary = gensim.corpora.Dictionary(texts)
    text_train = [dictionary.doc2bow(i) for i in texts]
    
    if len(texts) <=200:
        num_topics = 100
    else:
        num_topics = int(len(texts)/2)
        
        
        
    lda = gensim.models.ldamodel.LdaModel(corpus=text_train, num_topics=num_topics, id2word=dictionary, 
                                          distributed=False, chunksize=2000, passes=20, 
                                          update_every=1, alpha='auto', eta='auto', 
                                          decay=0.5, offset=1.0, eval_every=0, 
                                          iterations=1000, gamma_threshold=0.001, 
                                          minimum_probability=0, random_state=777, 
                                          ns_conf=None, minimum_phi_value=0, 
                                          per_word_topics=True, callbacks=None)
    lda.log_perplexity(text_train)
