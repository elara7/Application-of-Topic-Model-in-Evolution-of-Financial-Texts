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


# 初始K=300
texts =[i.split() for i in  test_corpus.loc[test_corpus['i']==6]['content']]
dictionary = gensim.corpora.Dictionary(texts)
bow = [dictionary.doc2bow(t) for t in texts]
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=150, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0 
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=225, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=262, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=281, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=271, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=276, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=278, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=279, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=280, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
#1 
# 279 , 9


# 初始K=300
texts =[i.split() for i in  test_corpus.loc[test_corpus['i']==1]['content']]
dictionary = gensim.corpora.Dictionary(texts)
bow = [dictionary.doc2bow(t) for t in texts]
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=150, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=75, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=37, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=18, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=9, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=4, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=6, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=7, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=8, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
# 7 , 9


# 初始K=300
texts =[i.split() for i in  test_corpus.loc[test_corpus['i']==2]['content']]
dictionary = gensim.corpora.Dictionary(texts)
bow = [dictionary.doc2bow(t) for t in texts]
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=150, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=75, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=37, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=18, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=27, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=22, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=24, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=25, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
# 24 , 8



# 初始K=300
texts =[i.split() for i in  test_corpus.loc[test_corpus['i']==3]['content']]
dictionary = gensim.corpora.Dictionary(texts)
bow = [dictionary.doc2bow(t) for t in texts]
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=150, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=75, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=37, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=56, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=46, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=41, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=43, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=42, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
# 42, 8

# 初始K=300
texts =[i.split() for i in  test_corpus.loc[test_corpus['i']==4]['content']]
dictionary = gensim.corpora.Dictionary(texts)
bow = [dictionary.doc2bow(t) for t in texts]
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=150, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=75, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=112, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=93, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=102, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=107, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=109, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=110, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
# 109 , 8



# 初始K=300
texts =[i.split() for i in  test_corpus.loc[test_corpus['i']==5]['content']]
dictionary = gensim.corpora.Dictionary(texts)
bow = [dictionary.doc2bow(t) for t in texts]
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=150, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=225, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=262, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=243, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=234, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=229, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=231, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 1
lda_f = gensim.models.ldamodel.LdaModel(corpus=bow, num_topics=230, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=20
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
print(find_reptopic(lda_f))
# 0
# 230 8