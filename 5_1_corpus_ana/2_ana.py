# -*- coding: utf-8 -*-
"""
Created on Thu Dec 21 01:10:20 2017

@author: elara
"""


import pickle
import platform

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'

f2 = open(main_path+'Elara/Documents/paper/corpus_ana/res/word_pos_dicts.txt',"rb")
word_pos_dicts = pickle.load(f2)
f2.close()

f2 = open(main_path+'Elara/Documents/paper/corpus_ana/res/word_len_dicts.txt',"rb")
word_len_dicts = pickle.load(f2)
f2.close()

f2 = open(main_path+'Elara/Documents/paper/corpus_ana/res/doc_len.txt',"rb")
doc_len = pickle.load(f2)
f2.close()


word_pos = [[k,v] for k,v in word_pos_dicts.items()]
word_len = [[k,v] for k,v in word_len_dicts.items()]

import pandas as pd
pd.DataFrame(word_pos).to_csv(main_path + 'Elara/Documents/paper/corpus_ana/res/word_pos.csv',index='None')
pd.DataFrame(word_len).to_csv(main_path + 'Elara/Documents/paper/corpus_ana/res/word_len.csv',index='None')
pd.DataFrame(doc_len).to_csv(main_path + 'Elara/Documents/paper/corpus_ana/res/doc_len.csv',index='None')
