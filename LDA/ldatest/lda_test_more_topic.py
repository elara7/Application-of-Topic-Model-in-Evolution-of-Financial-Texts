# -*- coding: utf-8 -*-
"""
Created on Tue Dec 19 16:18:57 2017

@author: elara
"""



import platform
import pandas as pd
from pandasql import sqldf 
import gensim
import logging
import multiprocessing
import itertools
import numpy as np
cores = multiprocessing.cpu_count()
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
test_corpus_path = main_path+ 'Elara/Documents/paper/LDA/lda_test.csv'
test_text = pd.read_csv(test_corpus_path,encoding='utf-8',engine='python',names = ['i','content'])

i = list(set(test_text['i']))[0]
texts =[i.split() for i in  test_text.loc[test_text['i']==i]['content']]
dictionary = gensim.corpora.Dictionary(texts)
text_train = [dictionary.doc2bow(i) for i in texts]
lda1 = gensim.models.ldamodel.LdaModel(corpus=text_train, num_topics=165, id2word=dictionary, 
                              distributed=False, chunksize=2000, passes=20
                              , alpha='auto', eta='auto', 
                              decay=0.5, offset=1.0, eval_every=0, 
                              iterations=1000, gamma_threshold=0.001, 
                              minimum_probability=0, random_state=777, 
                              ns_conf=None, minimum_phi_value=0, 
                              per_word_topics=True)


        
lda2 = gensim.models.ldamodel.LdaModel(corpus=text_train, num_topics=5, id2word=dictionary, 
                              distributed=False, chunksize=2000, passes=20
                              , alpha='auto', eta='auto', 
                              decay=0.5, offset=1.0, eval_every=0, 
                              iterations=1000, gamma_threshold=0.001, 
                              minimum_probability=0, random_state=777, 
                              ns_conf=None, minimum_phi_value=0, 
                              per_word_topics=True)

        
lda3 = gensim.models.ldamodel.LdaModel(corpus=text_train, num_topics=20, id2word=dictionary, 
                              distributed=False, chunksize=2000, passes=20
                              , alpha='auto', eta='auto', 
                              decay=0.5, offset=1.0, eval_every=0, 
                              iterations=1000, gamma_threshold=0.001, 
                              minimum_probability=0, random_state=777, 
                              ns_conf=None, minimum_phi_value=0, 
                              per_word_topics=True)


yyy = list(lda1.get_document_topics(text_train))
ym = [[j[1] for j in i] for i in yyy]
import pandas as pd
YMT  = pd.DataFrame(ym)
kkk = YMT.apply(lambda x: x.sum())


c = gensim.models.CoherenceModel(model=lda1, texts=texts, dictionary=dictionary, coherence='c_v')
c.get_coherence()

c = gensim.models.CoherenceModel(model=lda2, texts=texts, dictionary=dictionary, coherence='c_v')
c.get_coherence()

c = gensim.models.CoherenceModel(model=lda3, texts=texts, dictionary=dictionary, coherence='c_v')
c.get_coherence()
[10,50,100,150,200,250,300]
 [25,50,75,100,125,150,175,200,225,250,275,300]
res1 = {}
res2 = {}
trylist = [25,50,75,100,125,150,175,200,225,250,275,300]
for i in trylist:
    print(i)
    lda = gensim.models.ldamulticore.LdaMulticore(corpus=text_train, workers=4,
                                                  num_topics=i, id2word=dictionary, chunksize=2000, passes=20
                              , 
                              decay=0.5, offset=1.0, eval_every=0, 
                              iterations=2000, gamma_threshold=0.001, 
                              minimum_probability=0, random_state=777, minimum_phi_value=0)
    print(lda.bound(text_train))
    c = gensim.models.CoherenceModel(model=lda, texts=texts, coherence='c_v')
    res1[i]=c.get_coherence()
    print(res1[i])
    lda1 = gensim.models.ldamodel.LdaModel(corpus=text_train, num_topics=i, id2word=dictionary, 
                              distributed=False, chunksize=2000, passes=20
                              , alpha='auto', eta='auto', 
                              decay=0.5, offset=1.0, eval_every=0, 
                              iterations=1000, gamma_threshold=0.001, 
                              minimum_probability=0, random_state=777, 
                              ns_conf=None, minimum_phi_value=0, 
                              per_word_topics=True)
    print(lda1.bound(text_train))
    c = gensim.models.CoherenceModel(model=lda1, texts=texts, coherence='c_v')
    res2[i]=c.get_coherence()
    print(res2[i])

pyplot.plot([res2[i] for i in trylist])

m_r = np.mean(res.values())






std1 = {}
for i in range(200):
    ttt1 =[j[1] for j in lda1.get_topic_terms(topicid=i,topn=len(dictionary))]
    std1[i] = np.std(ttt1)
std_value1 = sorted(list(std1.values()),reverse=True)
stop_value1 = float('inf')
for i in range(len(std_value1)):
    if std_value1[i]<=1e-4:
        stop_value1 = std_value1[i]
        break
for i in std1.keys():
    if std1[i]<=stop_value1:
        std1[i] = None

stop_topic11 =[i for i in std1.keys() if std1[i] == None]

d1 = lda1.diff(lda1,distance='jensen_shannon')[0]

for x in range(d1.shape[0]):
    for y in range(x+1,d1.shape[1]):
        if d1[y,x] != 0:
            continue
        else:
            d1[y,:] = None
            d1[:,y] = None
            print('drop ',y)
stop_topic21 = []
for i in range(d1.shape[0]):
    if np.isnan(d1[i,0]):
        stop_topic21.append(i)
        
stop_topic1 = list(set(stop_topic11+stop_topic21))

std2 = {}
for i in range(300):
    ttt2 =[j[1] for j in lda2.get_topic_terms(topicid=i,topn=len(dictionary))]
    std2[i] = np.std(ttt2)
std_value2 = sorted(list(std2.values()),reverse=True)
stop_value2 = float('inf')
for i in range(len(std_value2)):
    if std_value2[i]<=1e-4:
        stop_value2 = std_value2[i]
        break
for i in std2.keys():
    if std2[i]<=stop_value2:
        std2[i] = None

stop_topic12 =[i for i in std2.keys() if std2[i] == None]

d2 = lda2.diff(lda2,distance='jensen_shannon')[0]

for x in range(d2.shape[0]):
    for y in range(x+1,d2.shape[1]):
        if d2[y,x] != 0:
            continue
        else:
            d2[y,:] = None
            d2[:,y] = None
            print('drop ',y)
stop_topic22 = []
for i in range(d2.shape[0]):
    if np.isnan(d2[i,0]):
        stop_topic22.append(i)
        
stop_topic2 = list(set(stop_topic12+stop_topic22))
            
        
        
        print('文本数',self.cl,'字典长度',self.dl,'主题数',nt,'迭代次数',it,'有重复',is_rep)
        return [self.cl]+[self.dl]+[nt]+[it]+[is_rep]+lda.metrics['Perplexity']       



res = []
pool = multiprocessing.Pool(processes = cores)
for i in :

    ttt = test_f(text_train,dictionary)
    
        
    temp = pool.map(ttt.test_f,para)
    
    res+=temp
            
            
            
pd.DataFrame(res).to_csv(main_path+'Elara/Documents/paper/LDA/lda_test_res.csv',header=None,index=None)