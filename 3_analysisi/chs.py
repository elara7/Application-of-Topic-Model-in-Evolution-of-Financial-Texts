# -*- coding: utf-8 -*-
"""
Created on Fri Dec  8 12:21:15 2017

@author: elara
"""


chs = []
for i in os.listdir('D:\\Elara\\Documents\\paper\\corpus\\'):
    urllist=pd.read_csv("D:\\Elara\\Documents\\paper\\corpus\\"+i, engine='python')
    ch={}
    for i in [i.split('.')[0].split('/')[-1] for i in urllist['1']]:
        try:
            ch[i] += 1
        except:
            ch[i] = 1
    chs.append(ch)
urllist=pd.read_csv("D:\\Elara\\Documents\\paper\\corpus\\"+'urllist东兴证券.csv', engine='python')
# stock中主要为市场数据，且证券行业股票会因为报告观点、对其他股票买入卖出，等多次被提到

chs = {}


    
ch_set = set([i.split('.')[0].split('/')[-1] for i in urllist['1']])
