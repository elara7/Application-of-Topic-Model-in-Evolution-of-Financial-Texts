# -*- coding: utf-8 -*-
"""
Created on Mon Dec 25 00:02:06 2017

@author: elara
"""




import platform
import pandas as pd
from pandasql import sqldf 
import re
import numpy as np
import copy
import multiprocessing
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
merged_corpus_path = main_path+ 'Elara/Documents/paper/merged_corpus/merged_result.csv'
merged_kv_corpus_path = main_path+ 'Elara/Documents/paper/merged_corpus/merged_kv_result.csv'

data_all = pd.read_csv(main_path + merged_kv_corpus_path,engine='python',)