# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 14:48:45 2018

@author: elara
"""


import platform
import pandas as pd
from pandasql import sqldf 
import multiprocessing
import copy
import gensim
import numpy as np
import os
import logging
import re
import scipy
import traceback
cores = multiprocessing.cpu_count()
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
    main_path2 = '/mnt/D/'
if platform.system() == 'Windows':
    main_path = 'D:/paper_c/'
    main_path2 = 'D:/'



tfidf_filter_corpus_path = main_path+ 'Elara/Documents/paper/9_0_tiidf/tfidf_filter/filterresult/'
lda_best_model_path = main_path2 + 'lda_run/bestmodel1/'
dict_path = main_path2 + 'lda_run/dic1/'

lda_name_list = os.listdir(lda_best_model_path)
model_name_list = list(set([i.split('.')[0] for i in lda_name_list]))
model_info = [[i.split('_')[0],i.split('_')[1],i.split('_')[2],i.split('_')[3]] for i in model_name_list]
stock_names = list(set([i[0] for i in model_info]))

stock_name = stock_names[0]

stage_list = sorted([int(i[1]) for i in model_info if i[0] == stock_name])
    
text_info = pd.read_csv(tfidf_filter_corpus_path + stock_name+'.csv',encoding='utf-8',engine='python',names = ['name','date','stage','content'])

stage = stage_list[20]

temp = text_info.loc[text_info['stage']==stage]
text = temp.sort_values(by=['date','content'])
text = text.dropna(axis=0)
text.index = range(len(text))
dictionary = gensim.corpora.Dictionary.load(dict_path + stock_name + str(stage))
texts = [temp.iloc[i,3].split() for i in range(len(temp)) if type(temp.iloc[i,3])==str]
corpus = [dictionary.doc2bow(text) for text in texts]

name_stop = [stock_name,stock_name[0:2],stock_name[2:4]]
topic_num = int([i[3] for i in model_info if i[0]==stock_name and i[1] ==str(stage)][0])
model_name = ['_'.join(i) for i in model_info if i[0]==stock_name and i[1] == str(stage)][0]
model_fit = gensim.models.ldamodel.LdaModel.load(lda_best_model_path + model_name)
model_fit.id2word = dictionary
x = 486
d_topic = model_fit.get_document_topics(corpus[x],minimum_probability=0.0001)
[[model_fit.print_topic(i[0],10),i[1]] for i in d_topic]
model_fit.print_topics()

raw_c = pd.read_csv('D:/paper_c/Elara/Documents/paper/2_corpus/urllist上汽集团.csv',engine='python',skiprows=1,
                                   names=['index','title','url','date','content','cate','source','conments','uv'], 
                                   encoding="utf-8")
raw_c = raw_c.sort_values(by=['date','content'])
