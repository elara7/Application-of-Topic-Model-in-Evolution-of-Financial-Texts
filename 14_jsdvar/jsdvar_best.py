# -*- coding: utf-8 -*-
"""
Created on Mon Feb 19 17:18:44 2018

@author: elara
"""


import pandas as pd
from pandasql import sqldf 
import gensim
import logging
#cores = multiprocessing.cpu_count()
cores = 4
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)
pysqldf = lambda q: sqldf(q, globals())

#if platform.system() == 'Linux':
#    main_path = '/mnt/c/'
#if platform.system() == 'Windows':
#    main_path = 'C:/'
    
#test_corpus_path = main_path+'Elara/Documents/paper/ldatest/test.csv'
test_corpus_path = 'D:/paper_c/Elara/Documents/paper/14_jsdvar/test.csv'

test_corpus = pd.read_csv(test_corpus_path,engine='python',encoding='utf-8',names=['i','content'])

def find_reptopic(lda_fit_model):
    is_rep = 0
    d = lda_fit_model.diff(lda_fit_model)[0]
    for x in range(len(d)):
        for y in range(x+1,len(d)):
            if d[y,x]==0:
                is_rep=1
                break
    return is_rep

paras = [25,50,100,300,300,300]

for i in list(set(test_corpus['i'])):
    texts =[i.split() for i in  test_corpus.loc[test_corpus['i']==i]['content']]
    dictionary = gensim.corpora.Dictionary(texts)
    bow = [dictionary.doc2bow(t) for t in texts]
    para = paras[i-1]
    lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=para, id2word=dictionary, 
                                      distributed=False, chunksize=2000, passes=20
                                      , alpha='auto', eta='auto', 
                                      decay=0.5, offset=1.0, eval_every=0, 
                                      iterations=500, gamma_threshold=0.001, 
                                      minimum_probability=0, random_state=777, 
                                      ns_conf=None, minimum_phi_value=0)
    print(find_reptopic(lda_f))

#res: 1 1 1 1 1 1