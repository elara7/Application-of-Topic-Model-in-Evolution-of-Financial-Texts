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
#cores = multiprocessing.cpu_count()
cores = 6
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
test_corpus_path = main_path+'Elara/Documents/paper/ldatest/test.csv'

test_corpus = pd.read_csv(test_corpus_path,engine='python',encoding='utf-8',names=['i','content'])

para = list(itertools.product([5,20,50,100,200,300],[500,1000,2000]))

class test_f(object):
    
    def __init__(self,text_train,dictionary):
        self.text_train = text_train
        #self.text_test = text_test
        self.dictionary = dictionary
        self.dl = len(self.dictionary)
        self.cl = len(self.text_train)
        #self.tl = len(self.text_test)
        
    def test_f(self,line):

        passes = 100
        nt = line[0]
        it = line[1]
        Perplexity_train = gensim.models.callbacks.PerplexityMetric(self.text_train,logger='shell')
        #Perplexity_test = gensim.models.callbacks.PerplexityMetric(self.text_test,logger='shell')
        self.lda = gensim.models.ldamodel.LdaModel(corpus=self.text_train, num_topics=nt, id2word=self.dictionary, 
                                      distributed=False, chunksize=2000, passes=passes
                                      , alpha='auto', eta='auto', 
                                      decay=0.5, offset=1.0, eval_every=0, 
                                      iterations=it, gamma_threshold=0.001, 
                                      minimum_probability=0, random_state=777, 
                                      ns_conf=None, minimum_phi_value=0, 
                                      per_word_topics=True, 
                                      callbacks=[Perplexity_train])
        is_rep = 0
        d = self.lda.diff(self.lda)[0]
        for x in range(len(d)):
            for y in range(x+1,len(d)):
                if d[y,x]==0:
                    is_rep=1
                    break
        print('文本数',self.cl,'字典长度',self.dl,'主题数',nt,'迭代次数',it,'有重复',is_rep)
        return [self.cl]+[self.dl]+[nt]+[it]+[is_rep]+self.lda.metrics['Perplexity']       



pool = multiprocessing.Pool(processes = cores)
for i in list(set(test_corpus['i'])):
    if i%2 != 0 :
        continue
    texts =[i.split() for i in  test_corpus.loc[test_corpus['i']==i]['content']]
    if len(texts) in (88,681):
        continue
    dictionary = gensim.corpora.Dictionary(texts)
    text_all = [dictionary.doc2bow(i) for i in texts]
    #random.shuffle(text_all)
    #text_train = text_all[0:int(len(text_all)/2)]
    #text_test = text_all[int(len(text_all)/2):]
    #print(len(text_train))
    #print(len(text_test))
    ttt = test_f(text_all,dictionary)
    temp = pool.map(ttt.test_f,para)
    
    pd.DataFrame(temp).to_csv(main_path+'Elara/Documents/paper/ldatest/'+str(len(text_all))+'lda_test_res.csv',header=None,index=None)