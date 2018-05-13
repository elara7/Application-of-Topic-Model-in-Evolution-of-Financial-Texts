# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 00:02:00 2017

@author: elara
"""
import os
import codecs
temp=[]

for i in os.listdir('C:/Elara/Documents/wordlib/other/txt/'):
    f = codecs.open('C:/Elara/Documents/wordlib/other/txt/'+i,mode='r',encoding='utf-8')
    temp += f.readlines()
    f.close()

other = list(set(temp))
f = open('C:/Elara/Documents/wordlib/other/other.txt',mode = 'w', encoding='utf-8')
for i in other:
    f.write(i.strip()+'\n')
f.close()
