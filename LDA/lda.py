# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 11:04:49 2017

@author: elara
"""



import platform
import pandas as pd
from pandasql import sqldf 
import multiprocessing
import copy
import gensim
import re
import numpy as np
import os
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
tfidf_filter_corpus_path = main_path+ 'Elara/Documents/paper/tiidf/tfidf_filter/filterresult/'

i = os.listdir(tfidf_filter_corpus_path)[0]
i

text_info = pd.read_csv(tfidf_filter_corpus_path + i,encoding='utf-8',engine='python',names = ['stage','name','date','title','url','content','ch','source','url_ch'])

text_all = []
stage_list = sorted(list(set(text_info['stage'])))
for i in stage_list:
    temp = text_info.loc[text_info['stage']==i]
    text_all.append([list(temp.iloc[j,:]) for j in range(len(temp))])

texts = [i[5].split() for i in text_all[0]]
# 向量化
dictionary = gensim.corpora.Dictionary(texts)
text_train = [dictionary.doc2bow(i) for i in texts]

lda = gensim.models.ldamulticore.LdaMulticore(corpus=text_train, num_topics=100, id2word=dictionary, workers=4, chunksize=2000, passes=20, batch=False, alpha='symmetric', eta=None, decay=0.5, offset=1.0, eval_every=0, iterations=1000, gamma_threshold=0.001, random_state=777, minimum_probability=0, minimum_phi_value=0, per_word_topics=True)
x1 = lda1.print_topics(num_topics=165, num_words=10)
x2 = lda2.print_topics(num_topics=300, num_words=10)

d = lda.diff(lda,distance='jensen_shannon')[0]

std = {}
for i in range(100):
    ttt =[i[1] for i in lda.get_topic_terms(topicid=i,topn=len(dictionary))]
    std[i] = np.std(ttt)
std_value = sorted(list(std.values()),reverse=True)
stop_value = float('inf')
for i in range(len(std_value)):
    if std_value[i]<=1e-4:
        stop_value = std_value[i]
        break
for i in std.keys():
    if std[i]<=stop_value:
        std[i] = None

stop_topic1 =[i for i in std.keys() if std[i] == None]

for x in range(d.shape[0]):
    for y in range(x+1,d.shape[1]):
        if d[y,x] != 0:
            continue
        else:
            d[y,:] = None
            d[:,y] = None
            print('drop ',y)
stop_topic2 = []
for i in range(d.shape[0]):
    if np.isnan(d[i,0]):
        stop_topic2.append(i)
        


stop_topic = list(set(stop_topic1+stop_topic2))


x = lda.print_topics(num_topics=100, num_words=10)
f = [[std[i],x[i][0],x[i][1]] for i in range(len(x)) if i not in stop_topic]

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
            
            
def grab_score(topic):
    uni_topic = [1/len(topic)]*len(topic)
    return jsd(topic,uni_topic)

grab = []
for i in range(100):
    if i in stop_topic:
        continue
    ttt =[i[1] for i in lda.get_topic_terms(topicid=i,topn=len(dictionary))]
    grab.append([i,grab_score(ttt)])
grab_data =[[i[1]] for i in grab]
from sklearn.cluster import KMeans
km = KMeans(2).fit(np.array(grab_data))
km.labels_
km.cluster_centers_
grab_list = [grab[i][0] for i in range(len(km.labels_)) if km.labels_[i] == 1]

lab1 =  [grab[i][1] for i in range(len(km.labels_)) if km.labels_[i] == 1]
lab0 =  [grab[i][1] for i in range(len(km.labels_)) if km.labels_[i] == 0]




f = [[std[i],x[i][0],x[i][1]] for i in range(len(x)) if i not in stop_topic and i not in grab_list]

from pprint import pprint  # pretty-printer
pprint(texts)

#logp=[]
#for i in range(1,50):
#    lda = gensim.models.ldamulticore.LdaMulticore(corpus=text_train, num_topics=50, id2word=dictionary, workers=4, chunksize=2000, passes=i, batch=False, alpha='symmetric', eta=None, decay=0.5, offset=1.0, eval_every=0, iterations=1000, gamma_threshold=0.001, random_state=777, minimum_probability=0, minimum_phi_value=0, per_word_topics=False)
#    logp.append([lda.log_perplexity(text_train),pow(2,-lda.log_perplexity(text_train))])
#
#bound = [i[0] for i in logp]
#perplexity = [i[1] for i in logp]
#
#pd.DataFrame({'bound':bound,'perplexity':perplexity}).to_csv(main_path+'Elara/Documents/paper/LDA/perplexity50.csv')
#from matplotlib import pyplot as plt
#plt.plot(range(1,50),bound)
#plt.plot(range(1,50),perplexity)




    