# -*- coding: utf-8 -*-
"""
Created on Mon Feb 19 17:18:44 2018

@author: elara
"""


import pandas as pd
from pandasql import sqldf 
import gensim
import logging
import multiprocessing
import itertools
import random
import numpy as np
import scipy.stats 
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

#para = list(itertools.product([25,50],[500]))
para = list(itertools.product([25,50,75,100,125,150,175,200,225,250,275,300],[500]))



def jsd(p,q):
    M = (p+q)/2
    return 0.5*scipy.stats.entropy(p, M)+0.5*scipy.stats.entropy(q, M)

class test_f(object):
    
    def __init__(self,bow_train,text_train,dictionary,bow_test,text_test):
        self.bow_train = bow_train
        self.text_train = text_train
        
        self.bow_test = bow_test
        self.text_test = text_test
        
        self.dictionary = dictionary
        self.dl = len(self.dictionary)
        self.cl = len(self.text_train)
        self.tl = len(self.text_test)

        
    def test_f(self,line):

        passes = 20
        nt = line[0]
        it = line[1]
        #Perplexity_test = gensim.models.callbacks.PerplexityMetric(self.text_test,logger='shell')
        self.lda = gensim.models.ldamodel.LdaModel(corpus=self.bow_train, num_topics=nt, id2word=self.dictionary, 
                                      distributed=False, chunksize=2000, passes=passes
                                      , alpha='auto', eta='auto', 
                                      decay=0.5, offset=1.0, eval_every=0, 
                                      iterations=it, gamma_threshold=0.001, 
                                      minimum_probability=0, random_state=777, 
                                      ns_conf=None, minimum_phi_value=0)
        
#        ldaf = gensim.models.ldamodel.LdaModel(corpus=bow_train, num_topics=nt, id2word=dictionary, 
#                              distributed=False, chunksize=2000, passes=passes
#                              , alpha='auto', eta='auto', 
#                              decay=0.5, offset=1.0, eval_every=0, 
#                              iterations=it, gamma_threshold=0.001, 
#                              minimum_probability=0, random_state=777, 
#                              ns_conf=None, minimum_phi_value=0)

#        d_t = np.array([[topic[1] for topic in doc] for doc in list(ldaf.get_document_topics(bow_test))])
#        t_w = ldaf.get_topics()
#        d_w = np.dot(d_t,t_w)
        d_t = np.array([[topic[1] for topic in doc] for doc in list(self.lda.get_document_topics(self.bow_test))])
        t_w = self.lda.get_topics()
        d_w = np.dot(d_t,t_w)
        
        log_p = 0
        Nt = 0
        for doc_index , doc_t in enumerate(self.bow_test):
            for term_t in doc_t:
                term_index = term_t[0]
                term_freq = term_t[1]
                
                Nt += term_freq
                log_p += np.log(d_w[doc_index,term_index])*term_freq
        
        perplexity = np.exp(-log_p/Nt)
        
        phi_mean = np.mean(t_w,axis=0)
        jsdvar = 0
        for topic_index in range(t_w.shape[0]):
            jsdvar+=pow(jsd(t_w[topic_index,],phi_mean),2)/t_w.shape[0]
        
        result = perplexity/jsdvar
     
        print('文本数',self.cl,'字典长度',self.dl,'主题数',nt,'迭代次数',it,'perplexity',perplexity,'jsdvar',jsdvar,'result',result)
        return [self.cl]+[self.dl]+[nt]+[it]+[perplexity]+[jsdvar]+[result]       
#        print('文本数',self.cl,'字典长度',self.dl,'主题数',nt,'迭代次数',it)
#        return [self.cl]+[self.dl]+[nt]+[it]    

if __name__ == '__main__':
    
    for i in list(set(test_corpus['i'])):
        texts =[i.split() for i in  test_corpus.loc[test_corpus['i']==i]['content']]
        if len(texts)<=340:
            continue
        dictionary = gensim.corpora.Dictionary(texts)
    
        random.shuffle(texts)
        text_train = texts[0:int(len(texts)/2)]
        text_test = texts[int(len(texts)/2):]
        
        bow_train = [dictionary.doc2bow(t) for t in text_train]
        bow_test = [dictionary.doc2bow(t) for t in text_test]
        print(len(text_train))
        print(len(text_test))
        ttt = test_f(bow_train,text_train,dictionary,bow_test,text_test)
        pool = multiprocessing.Pool(processes = cores)
        temp = pool.map(ttt.test_f,para)
        pool.close()
        pd.DataFrame(temp).to_csv('D:/paper_c/Elara/Documents/paper/14_jsdvar/'+str(len(text_train))+'jsdvar.csv',header=None,index=None)