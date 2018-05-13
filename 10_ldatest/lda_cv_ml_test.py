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
import random
#cores = multiprocessing.cpu_count()
cores = 8
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
test_corpus_path = main_path+'Elara/Documents/paper/ldatest/test.csv'

test_corpus = pd.read_csv(test_corpus_path,engine='python',encoding='utf-8',names=['i','content'])

para = list(itertools.product([25,50,75,100,125,150,175,200,225,250,275,300],[1000]))

class test_f(object):
    
    def __init__(self,bow_train,text_train,dictionary):
        self.bow_train = bow_train
        self.text_train = text_train
        
        #self.bow_test = bow_test
        #self.text_test = text_test
        
        self.dictionary = dictionary
        self.dl = len(self.dictionary)
        self.cl = len(self.text_train)
        #self.tl = len(self.text_test)
        
    def test_f(self,line):

        passes = 20
        nt = line[0]
        it = line[1]
        #Perplexity_test = gensim.models.callbacks.PerplexityMetric(self.text_test,logger='shell')
        self.lda = gensim.models.ldamulticore.LdaMulticore(corpus=self.bow_train,workers=8, num_topics=nt, id2word=self.dictionary, 
                                      chunksize=2000, passes=passes
                                      , eval_every=0, 
                                      iterations=it, 
                                      minimum_probability=0, random_state=777, 
                                      minimum_phi_value=0)

        cm_train = gensim.models.coherencemodel.CoherenceModel(model=self.lda, texts=self.text_train, dictionary=self.dictionary, coherence='c_v',processes=1)
        cm_list_train = cm_train.get_coherence_per_topic()
        cm_avg_train = cm_train.get_coherence()
#        
#        cm_test = gensim.models.coherencemodel.CoherenceModel(model=self.lda, texts=self.text_test, dictionary=self.dictionary, coherence='c_v',processes=1)
#        cm_list_test = cm_test.get_coherence_per_topic()
#        cm_avg_test = cm_test.get_coherence()
#        
        #        cm_test = gensim.models.coherencemodel.CoherenceModel(model=self.lda, texts=self.text_test, dictionary=self.dictionary, coherence='c_v',processes=1)

#        print('文本数',self.cl,'字典长度',self.dl,'主题数',nt,'迭代次数',it,'cm_train_avg',cm_avg_train,'cm_test_avg',cm_avg_test)
#        return [self.cl]+[self.dl]+[nt]+[it]+[cm_avg_train]+[' '.join(cm_list_train)]+[cm_avg_test]+[' '.join(cm_list_test)]      
        print('文本数',self.cl,'字典长度',self.dl,'主题数',nt,'迭代次数',it,'cm_train_avg',cm_avg_train)
        return [self.cl]+[self.dl]+[nt]+[it]+[cm_avg_train]+cm_list_train  
#        print('文本数',self.cl,'字典长度',self.dl,'主题数',nt,'迭代次数',it)
#        return [self.cl]+[self.dl]+[nt]+[it]    

if __name__ == '__main__':
    
    for i in list(set(test_corpus['i'])):
        texts =[i.split() for i in  test_corpus.loc[test_corpus['i']==i]['content']]
#        if len(texts)<=200:
#            continue
        dictionary = gensim.corpora.Dictionary(texts)
    
        random.shuffle(texts)
        #text_train = texts[0:int(len(texts)/2)]
        #text_test = texts[int(len(texts)/2):]
        text_train = texts
        bow_train = [dictionary.doc2bow(t) for t in text_train]
        #bow_test = [dictionary.doc2bow(t) for t in text_test]
        print(len(text_train))
        #print(len(text_test))
        ttt = test_f(bow_train,text_train,dictionary)
        temp = []
#        pool = multiprocessing.Pool(processes = cores)
#        temp = pool.map(ttt.test_f,para)
#        pool.close()
        for j in para:
            temp.append(ttt.test_f(j))
        pd.DataFrame(temp).to_csv(main_path+'Elara/Documents/paper/ldatest/cmm/'+str(len(text_train))+'cm_test_res.csv',header=None,index=None)