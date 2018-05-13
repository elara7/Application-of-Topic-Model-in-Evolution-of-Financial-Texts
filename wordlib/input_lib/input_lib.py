# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 00:02:00 2017

@author: elara
"""
import os
import codecs
temp=[]

for i in os.listdir('C:/Elara/Documents/wordlib/input_lib/txt/'):
    f = codecs.open('C:/Elara/Documents/wordlib/input_lib/txt/'+i,mode='r',encoding='utf-8')
    temp += f.readlines()
    f.close()

input_word = list(set(temp))
f = open('C:/Elara/Documents/wordlib/input_lib/input_word.txt',mode = 'w', encoding='utf-8')
for i in input_word:
    f.write(i.strip()+'\n')
f.close()



