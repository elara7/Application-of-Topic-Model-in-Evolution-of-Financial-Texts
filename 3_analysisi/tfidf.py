# -*- coding: utf-8 -*-
"""
Created on Sun Dec 17 13:09:02 2017

@author: elara
"""
from matplotlib.font_manager import FontManager
from pylab import mpl
import subprocess

def get_matplot_zh_font():
    fm = FontManager()
    mat_fonts = set(f.name for f in fm.ttflist)

    output = subprocess.check_output('fc-list :lang=zh -f "%{family}\n"', shell=True)
    print(output)
    zh_fonts = set(f.split(',', 1)[0] for f in output.decode('utf-8').split('\n'))
    available = list(mat_fonts & zh_fonts)

    print('*' * 10, '可用的字体', '*' * 10)
    for f in available:
        print(f)
    return available

def set_matplot_zh_font():
    available = get_matplot_zh_font()
    if len(available) > 0:
        mpl.rcParams['font.sans-serif'] = [available[0]]    # 指定默认字体
        mpl.rcParams['axes.unicode_minus'] = False          # 解决保存图像是负号'-'显示为方块的问题
set_matplot_zh_font()

import platform
import pandas as pd
from pandasql import sqldf 
import re
import numpy as np
import copy
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
merged_corpus_path = main_path+ 'Elara/Documents/paper/merged_corpus/merged_result.csv'
merged_kv_corpus_path = main_path+ 'Elara/Documents/paper/merged_corpus/merged_kv_result.csv'
def word_filter(word):
    return re.search(r'^[0-9]+$',word)
# 载入
print('load merge corpus')
merge_corpus = pd.read_csv(merged_corpus_path, names=['name','date','title','url','content','cate','source','conments','uv','url_ch',\
                                                     'volume','is_split_2016_1','is_split_2016_2','is_split_2016_3',\
                                                     'is_split_all_1','is_split_all_2','is_split_all_3'])
print('nrow = ',len(merge_corpus))

#取出一个资产的文本
text_stage = merge_corpus.loc[(merge_corpus['name'] == '上汽集团') & (merge_corpus['date'] >= 20150306)]
#找到非短间隔部分
text_long = text_stage.loc[text_stage['is_split_all_1'] == 3]
date_range = text_long['date']
start = min(date_range)
end = max(date_range)
print('start = ',start,' end = ',end)
# 去重 list化
q2 = 'select distinct name, date, title, url,content, cate,source, url_ch from merge_corpus where date <= '+str(end)+' and date >= '+str(start)+' and content is not null and title is not null'
text_all = pysqldf(q2)
text_all_list = []
for i in range(len(text_all)):
    text_all_list.append(list(text_all.iloc[i,:]))


class adj_tfidf(object):
    def __init__(self,texts,title=0,content=1,a=0.1):
        self.texts_content_df = self.cal_df([i[content].split() for i in texts])
        self.texts_title_df = self.cal_df([i[title].split() for i in texts])
        
        #self.texts_tf = [[self.cal_tf(i[title].split()),self.cal_tf(i[content].split())] for i in texts]
        self.a = a
            
        
    def cal_df(self,texts):
        df = {}
        doc_num = len(texts)
        for i in texts:
            for j in list(set(i)):
                if j in df.keys():
                    df[j] += 1/doc_num
                else:
                    df[j] = 1/doc_num
        return df


    def cal_tf(self,text):
        tf = {}
        text_len = len(text)
        for i in text:
            if i in tf.keys():
                tf[i] += 1/text_len
            else:
                tf[i] = 1/text_len
        return tf
    
    def adjusted_tfidf(self,text,title=0,content=1):
        title_tf_dict = self.cal_tf(text[title].split())
        content_tf_dict = self.cal_tf(text[content].split())
        tfidf = {}
        keys = list(title_tf_dict.keys()) + list(content_tf_dict.keys())
        for j in keys:
            
            if j in title_tf_dict.keys():
                title_tf = title_tf_dict[j]
            else:
                title_tf = 0
                
            if j in content_tf_dict.keys():
                content_tf = content_tf_dict[j]
            else:
                content_tf = 0
                
            adjusted_tf = self.a * title_tf + (1- self.a) * content_tf
                
            if j in self.texts_title_df.keys():
                i_title_df = np.log(1/self.texts_title_df[j]+0.01)
            else:
                i_title_df = 0
                
            if j in self.texts_content_df.keys():
                i_content_df = np.log(1/self.texts_content_df[j]+0.01)
            else:
                i_content_df = 0
                
            adjusted_idf = self.a * i_title_df + (1-self.a) * i_content_df
            tfidf[j] = adjusted_tf*adjusted_idf
        return tfidf

    def content_tfidf(self,text,title=0,content=1):
        content_tf_dict = self.cal_tf(text[content].split())
        content_tfidf = {}
        keys = list(content_tf_dict.keys())
        for j in keys:
                
            if j in content_tf_dict.keys():
                content_tf = content_tf_dict[j]
            else:
                content_tf = 0
                
            if j in self.texts_content_df.keys():
                i_content_df = np.log(1/self.texts_content_df[j]+0.01)
            else:
                i_content_df = 0

            content_tfidf[j] = content_tf*i_content_df
        return content_tfidf
    
    def title_tfidf(self,text,title=0,content=1):
        title_tf_dict = self.cal_tf(text[title].split())
        title_tfidf = {}
        keys = list(title_tf_dict.keys())
        for j in keys:
            
            if j in title_tf_dict.keys():
                title_tf = title_tf_dict[j]
            else:
                title_tf = 0
                
            if j in self.texts_title_df.keys():
                i_title_df = np.log(1/self.texts_title_df[j]+0.01)
            else:
                i_title_df = 0
                
            title_tfidf[j] = title_tf*i_title_df
        return title_tfidf
       
    
ttt = adj_tfidf(text_all_list,2,4)
ttt.texts_title_df['亿元']
all_tfidf=[]
for i in text_all_list:
    all_tfidf += list(ttt.adjusted_tfidf(i,1,3).values())

stock_tfidf=[]
for i in text_all_list:
    if i[0] != '上汽集团':
        continue
    else:
        stock_tfidf.append(ttt.adjusted_tfidf(i,2,4))
        
stock_content_tfidf=[]
for i in text_all_list:
    if i[0] != '上汽集团':
        continue
    else:
        stock_content_tfidf.append(ttt.content_tfidf(i,2,4))

word_tfidfs = {}
for i in stock_content_tfidf:
    for j in i.keys():
        if j in word_tfidfs.keys():
            word_tfidfs[j].append(i[j])
        else:
            word_tfidfs[j] = [i[j]]
rrrrr = []
word_tfidfs_rank = {}
for i in stock_content_tfidf:
    temp = copy.deepcopy(i)
    v = list(temp.values())
    v_len = len(v)
    v = sorted(v)
    for x in temp.keys():
        temp[x] = (v.index(temp[x])+1)/v_len
    for j in temp.keys():
        if j in word_tfidfs_rank.keys():
            word_tfidfs_rank[j].append(temp[j])
        else:
            word_tfidfs_rank[j] = [temp[j]]
            
word_tfidfs_norm = {}
for i in stock_content_tfidf:
    temp = copy.deepcopy(i)
    v = list(temp.values())
    max_v = max(v)
    min_v = min(v)
    diff_v = max_v-min_v
    for x in temp.keys():
        temp[x] = (temp[x]-min_v)/diff_v
    for j in temp.keys():
        if j in word_tfidfs_norm.keys():
            word_tfidfs_norm[j].append(temp[j])
        else:
            word_tfidfs_norm[j] = [temp[j]]

sum([len(i) for  i in stock_content_tfidf]) == sum([len(word_tfidfs[i]) for i in word_tfidfs.keys()])

sum([len(i) for  i in stock_content_tfidf]) == sum([len(word_tfidfs_rank[i]) for i in word_tfidfs_rank.keys()])

sum([len(i) for  i in stock_content_tfidf]) == sum([len(word_tfidfs_norm[i]) for i in word_tfidfs_norm.keys()])

document_N = len(stock_content_tfidf)
for i in word_tfidfs.keys():
    if len(word_tfidfs[i])>=document_N/2:
        word_tfidfs[i].remove(max(word_tfidfs[i]))
max(word_tfidfs['亿元'])

rrr = word_tfidfs_rank['亿元']
np.median(rrr)
plt.hist(rrr,np.arange(0,1,0.001))

box_data=[]
name=[]
name.index('亿元')
for i in word_tfidfs_norm.keys():
    box_data.append(word_tfidfs_norm[i])
    name.append(i)
med_ = [np.median(i) for i in box_data]
np.mean(med_)
ppp = pd.DataFrame({'name':name,'avg':avg_})
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
np.random.seed(2)  #设置随机种子
df = pd.DataFrame(np.random.rand(5,4),
columns=['A', 'B', 'C', 'D'])#先生成0-1之间的5*4维度数据，再装入4列DataFrame中
df.boxplot() #也可用plot.box()
plt.show()

a=range(1,10)
plt.boxplot(box_data[5:17],labels=name[5:17])

np.percentile(word_tfidfs_norm['亿元'],75)
import numpy as np    
all_avg = np.mean(all_tfidf)
all_sd = np.std(all_tfidf)
all_med = np.median(all_tfidf)
all_max = max(all_tfidf)
all_min = min(all_tfidf)
all25 = np.percentile(all_tfidf,25)
from matplotlib import pyplot as plt
plt.hist(all_tfidf,bins=np.arange(0,0.003,0.00001))
all_t = all_avg - 3*all_sd

res = ttt.adjusted_tfidf(text_all_list[1],1,3)
plt.hist(list(res.values()),bins=np.arange(0,0.1,0.001))
np.mean(list(res.values()))
np.percentile(list(res.values()),25)
min(list(res.values()))
ori = text_all_list[1]
#取出一个资产的文本
text_stage = merge_corpus.loc[(merge_corpus['name'] == '浦发银行') & (merge_corpus['date'] >= 20150306)]
#找到非短间隔部分
text_long = text_stage.loc[text_stage['is_split_all_1'] == 3]
#list化
text_long_list =[list(text_long.iloc[i,:]) for i in range(len(text_long))]
#去掉空文本
text_full = [i for i in text_long_list if type(i[4]) == str]
# 获取纯语料
texts = [i[4].split() for i in text_full]
# 统计词频
frequency_dict = {}
for i in texts:
    for j in i:
        if j in frequency_dict.keys():
            frequency_dict[j] += 1
        else:
            frequency_dict[j] =1 
# 删除出现2次及以下的词            
texts = [[token for token in text if frequency_dict[token] > 2 and word_filter(token) == None] for text in texts]
            
#获取对应时间范围
date_range = text_long['date']
start = min(date_range)
end = max(date_range)
print('start = ',start,' end = ',end)

#获取对应全部文本去重
q2 = 'select distinct content from merge_corpus where date <= '+str(end)+' and date >= '+str(start)+' and content is not null'
text_stage_all = pysqldf(q2)
text_stage_all_list = [list(text_stage_all.iloc[i])[0].split() for i in range(len(text_stage_all))]
# 计算df
df = {}
N = len(text_stage_all_list)
for i in text_stage_all_list:
    for j in list(set(i)):
        if j in df.keys():
            df[j] += 1/N
        else:
            df[j] =1/N
#计算tf
tfidf = []         
for i in texts:
    N = len(i)
    line_dict = {}
    for j in i:
        if j in line_dict.keys():
            line_dict[j] += (1/N)*np.log(1/df[j])
        else:
            line_dict[j] = (1/N)*np.log(1/df[j])
    tfidf.append(line_dict)


            
print('saving')
f1 = open(main_path+ 'Elara/Documents/paper/analysisi/all_frequency_dict.txt',"wb")
pickle.dump(all_frequency_dict, f1)
f1.close()