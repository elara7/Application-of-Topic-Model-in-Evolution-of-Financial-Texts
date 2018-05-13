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
cores = multiprocessing.cpu_count()
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
test_corpus_path = main_path+ 'Elara/Documents/paper/LDA/lda_test.csv'
test_text = pd.read_csv(test_corpus_path,encoding='utf-8',engine='python',names = ['i','content'])

para = list(itertools.product([5,20,50,100,200],[500,1000,2000]))

class test_f(object):
    
    def __init__(self,text_train,dictionary):
        self.text_train = text_train
        self.dictionary = dictionary
        self.dl = len(self.dictionary)
        self.cl = len(self.text_train)
        
    def test_f(self,line):

        passes = 100
        nt = line[0]
        it = line[1]
        Perplexity = gensim.models.callbacks.PerplexityMetric(self.text_train,logger='shell')
        lda = gensim.models.ldamodel.LdaModel(corpus=self.text_train, num_topics=nt, id2word=self.dictionary, 
                                      distributed=False, chunksize=2000, passes=passes
                                      , alpha='auto', eta='auto', 
                                      decay=0.5, offset=1.0, eval_every=0, 
                                      iterations=it, gamma_threshold=0.001, 
                                      minimum_probability=0, random_state=777, 
                                      ns_conf=None, minimum_phi_value=0, 
                                      per_word_topics=True, 
                                      callbacks=[Perplexity])
        is_rep = 0
        d = lda.diff(lda)[0]
        for x in range(len(d)):
            for y in range(x+1,len(d)):
                if d[y,x]==0:
                    is_rep=1
                    break
        print('文本数',self.cl,'字典长度',self.dl,'主题数',nt,'迭代次数',it,'有重复',is_rep)
        return [self.cl]+[self.dl]+[nt]+[it]+[is_rep]+lda.metrics['Perplexity']       



res = []
pool = multiprocessing.Pool(processes = cores)
for i in list(set(test_text['i'])):
    texts =[i.split() for i in  test_text.loc[test_text['i']==i]['content']]
    dictionary = gensim.corpora.Dictionary(texts)
    text_train = [dictionary.doc2bow(i) for i in texts]
    ttt = test_f(text_train,dictionary)
    
        
    temp = pool.map(ttt.test_f,para)
    
    res+=temp
            
            
            
pd.DataFrame(res).to_csv(main_path+'Elara/Documents/paper/LDA/lda_test_res.csv',header=None,index=None)