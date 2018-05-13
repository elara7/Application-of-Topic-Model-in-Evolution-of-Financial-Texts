# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 00:33:42 2017

@author: elara
"""


import pandas as pd
import os
import codecs
import re
wordlib_path = 'C:/Elara/Documents/wordlib/'

## dataduoduo
#f = open(wordlib_path + 'dataduoduo\\中文分词数据_词库_财经金融词汇大全（47976条）.txt')
#dataduoduo = f.readlines()
#f.close()
#finance_word = [i.split()[0].strip() for i in dataduoduo]


# THUOCL
thuocl_word=[]
for i in os.listdir(wordlib_path + 'THUOCL\\user\\'):
    f = codecs.open(wordlib_path + 'THUOCL\\user\\' + i,encoding='utf-8')
    thuocl = f.readlines()
    f.close()
    thuocl_word += [i.split()[0].strip() for i in thuocl]


# stockname
f = open(wordlib_path + 'stockname\\gainian.txt')
gainian = f.readlines()
f.close()
gainian_word = [z.strip() for z in [x for j in [i.split() for i in gainian] for x in j]]

bankuai_word = [ i.strip() for i in pd.read_csv(wordlib_path + 'stockname\\bankuai.csv',header = None)[0]]
industry_name_word = [ i.strip() for i in pd.read_csv(wordlib_path + 'stockname\\industry_name.csv',header = None)[0]]
stock_name_word = [ i.strip() for i in pd.read_csv(wordlib_path + 'stockname\\name_list.csv',header = None)[0]]

#
## input word
#f = codecs.open(wordlib_path + 'input_lib/input_word.txt',encoding='utf-8')
#input_word = [i.strip() for i in f.readlines()]
#f.close()

## other word
#f = codecs.open(wordlib_path + 'other/other.txt',encoding='utf-8')
#other = [i.strip() for i in f.readlines()]
#f.close()
#
#user_word = list(set(finance_word+thuocl_word+gainian_word+bankuai_word+industry_name_word+stock_name_word+input_word+other))
user_word = list(set(thuocl_word+gainian_word+bankuai_word+industry_name_word+stock_name_word))

def remove_punctuation(line):
    rule = re.compile(r"\(.+\)")
    line = rule.sub('',line)
    return line

user_word = [remove_punctuation(i).strip() for i in user_word]
 

user_word = [i.strip() for i in user_word if len(i.strip())>1 and len(i.strip())<=20]

f = codecs.open(wordlib_path + 'user_word.txt',mode = 'w', encoding='utf-8')
for i in user_word:
    f.write(i+'\n')
f.close()