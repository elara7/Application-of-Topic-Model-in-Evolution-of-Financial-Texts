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
import os
#cores = multiprocessing.cpu_count()
cores = multiprocessing.cpu_count()
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
    
tfidf_filter_corpus_path = main_path+ 'Elara/Documents/paper/9_0_tiidf/tfidf_filter/filterresult/'
dataset = []
for i in os.listdir(tfidf_filter_corpus_path):
    temp = pd.read_csv(tfidf_filter_corpus_path + i,encoding='utf-8',engine='python',names = ['stage','name','date','title','url','content','ch','source','url_ch'])
    for j in list(set(temp['stage'])):
        dataset.append( [text.split() for text in list(temp[temp['stage']==j]['content'])] )


para = list(itertools.product(dataset,[5,20,50,100,150,200,250,300]))

    
def test_f(input_para):
    text = input_para[0]
    topicnum = input_para[1]
    cl = len(text)
    if cl >=2000 and cl <=2500:
        passes = 15
    elif cl > 2500:
        passes = 10
    else:
        passes = 20
    dictionary = gensim.corpora.Dictionary(text)
    dl = len(dictionary)
    text_all = [dictionary.doc2bow(i) for i in text]
    lda = gensim.models.ldamodel.LdaModel(corpus=text_all, num_topics=topicnum, id2word=dictionary, 
                                  distributed=False, chunksize=2000, passes=passes
                                  , alpha='auto', eta='auto', 
                                  decay=0.5, offset=1.0, eval_every=0, 
                                  iterations=500, gamma_threshold=0.001, 
                                  minimum_probability=0, random_state=777, 
                                  ns_conf=None, minimum_phi_value=0)
    is_rep = 0
    d = lda.diff(lda)[0]
    for x in range(len(d)):
        for y in range(x+1,len(d)):
            if d[y,x]==0:
                is_rep=1
                break
    print('文本数',cl,'字典长度',dl,'主题数',topicnum,'有重复',is_rep)
    return [cl]+[dl]+[topicnum]+[is_rep]



pool = multiprocessing.Pool(processes = cores)

start = 0
end = 100
for i in range(len(para)//100+1):
    res = []
    print('running ',start,'to',end-1,'with chunk size ',len(para[start:end]))
    res = pool.map(test_f,para[start:end])
    pd.DataFrame(res).to_csv(main_path+'Elara/Documents/paper/10_ldatest/all_res/'+str(i)+'.csv',header=None,index=None,encoding='utf-8')
    start = end
    end+=100
        
