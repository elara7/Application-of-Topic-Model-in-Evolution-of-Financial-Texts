# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 14:33:23 2017

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
    
merged_corpus_path = main_path+ 'Elara/Documents/paper/8_merged_corpus/merged_result.csv'
merged_kv_corpus_path = main_path+ 'Elara/Documents/paper/8_merged_corpus/merged_kv_result.csv'

def word_filter(word):
    return re.search(r'^[0-9]+$',word)

#计算每个词分位数
def word_tfidfs_percentile(word_tfidfs_dict,p):
    word_tfidfs_percentile = {}
    for i in word_tfidfs_dict.keys():
        word_tfidfs_percentile[i] = np.percentile(word_tfidfs_dict[i],p)
    return word_tfidfs_percentile

def word_tfidfs_max(word_tfidfs_dict):
    word_tfidfs_max_dict = {}
    for i in word_tfidfs_dict.keys():
        word_tfidfs_max_dict[i] = max(word_tfidfs_dict[i])
    return word_tfidfs_max_dict

def word_tfidfs_min(word_tfidfs_dict):
    word_tfidfs_min_dict = {}
    for i in word_tfidfs_dict.keys():
        word_tfidfs_min_dict[i] = min(word_tfidfs_dict[i])
    return word_tfidfs_min_dict

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
 


# 载入
print('load merge corpus')
merge_corpus = pd.read_csv(merged_corpus_path, names=['name','date','title','url','content','cate','source','conments','uv','url_ch',\
                                                     'volume','is_split'])
print('nrow = ',len(merge_corpus))


stock_names = list(set(merge_corpus['name']))


class get_filtered_corpus(object):
    
    def get_filter_result(self,i):
        words = i[5].split()
        med = np.median(list(i[0].values()))
        re_words = []
        for j in words:
            if j in self.all_stop_words:
                continue
            elif j in self.all_keep_words:
                re_words.append(j)
            elif i[0][j] < med:
                continue
            else:
                re_words.append(j)
        if len(re_words)==0:
            print(i[3],'filter to 0 len')
            print(i[3],'dict',i[0])
            print(i[3],'url',i[4])
        i[5]=' '.join(re_words)
        return [self.stage_index]+i[1:]
    
    def get_filter_num(self,i):
        i[2] = ' '.join([j for j in i[2].split() if word_filter(j) == None])
        i[4] = ' '.join([j for j in i[4].split() if word_filter(j) == None])
        return i
            
    def get_filtered_corpus(self,stock_name):
        #stock_name = stock_names[0]
        stock_name = stock_name
        #取出一个资产一个阶段的文本
        cores = int(multiprocessing.cpu_count())
        pool = multiprocessing.Pool(processes = cores)
        print(stock_name,'set cores = ',cores)
        max_stage = max(merge_corpus.loc[merge_corpus['name']==stock_name]['is_split'])
        print(stock_name,'max_stage = ',max_stage)
        self.stage_index = 0
        final_result = []
        while 1:
            if self.stage_index > max_stage:
                break
            
            stock_text_stage = []
            while 1:
                if self.stage_index > max_stage:
                    break
                q1 = 'select distinct name, date, title, url,content, cate,source, url_ch from merge_corpus where content is not null and  name = "'+str(stock_name)+'" and is_split = '+str(self.stage_index)
                print('running ',q1)
                stock_text_stage = pysqldf(q1)
                if len(stock_text_stage) >= 1 and self.stage_index <= max_stage:
                    print(stock_name,' start is_split = ',self.stage_index)
                    break
                else:
                    self.stage_index+=1
            
            if len(stock_text_stage) == 0:
                break
            start = min(stock_text_stage['date'])
            end = max(stock_text_stage['date'])
            print('chunk of '+stock_name+' stage '+str(self.stage_index)+' start from '+str(start)+' until '+str(end))
            
            ##取出对应时间内的所有文本
            q2 = 'select distinct name, date, title, url,content, cate,source, url_ch from merge_corpus where date <= '+str(end)+' and date >= '+str(start)+' and content is not null and title is not null'
            print('running ',q2)
            text_all = pysqldf(q2)
            text_all_list = []
            for i in range(len(text_all)):
                temp_line = list(text_all.iloc[i,:])
                temp_line[4] = ' '.join([temp_line[2],temp_line[2],temp_line[4]])
                text_all_list.append(temp_line)
            
            print(stock_name+' stage '+str(self.stage_index),'total corpus = ',len(text_all_list))
            #去掉纯数字词
            print(stock_name+' stage '+str(self.stage_index),'filtering num')
#            for i in range(len(text_all_list)):
#                text_all_list[i][2] = ' '.join([j for j in text_all_list[i][2].split() if word_filter(j) == None])
#                text_all_list[i][4] = ' '.join([j for j in text_all_list[i][4].split() if word_filter(j) == None])
            text_all_list = pool.map(self.get_filter_num,text_all_list)
                
            # 计算df
            print(stock_name+' stage '+str(self.stage_index),'initial adj_tfidf')
            text_tfidf = adj_tfidf(text_all_list,2,4)
            
            # 计算全部tfidf
            print(stock_name+' stage '+str(self.stage_index),'cal tfidf')
            stock_content_tfidf=[]
            for i in text_all_list:
                if i[0] != stock_name:
                    continue
                else:
                    stock_content_tfidf.append([text_tfidf.content_tfidf(i,2,4)]+i)
                    
            print(stock_name+' stage '+str(self.stage_index),'stock corpus = ',len(stock_content_tfidf))
            
            # 汇总每个词tfidf
            print(stock_name+' stage '+str(self.stage_index),'cal perword tfidf')
            word_tfidfs = {}
            for i in stock_content_tfidf:
                for j in i[0].keys():
                    if j in word_tfidfs.keys():
                        word_tfidfs[j].append(i[0][j])
                    else:
                        word_tfidfs[j] = [i[0][j]]
            print(stock_name+' stage '+str(self.stage_index),'check len:',sum([len(i[0]) for  i in stock_content_tfidf]) == sum([len(word_tfidfs[i]) for i in word_tfidfs.keys()]))
             
            # 计算每个词中位数
            word_tfidfs_med = {}
            for i in word_tfidfs.keys():
                word_tfidfs_med[i] = np.median(word_tfidfs[i])
            
            #计算所有中位数的中位数
            med_med = np.median(list(word_tfidfs_med.values()))
            print(stock_name+' stage '+str(self.stage_index),'med_med = ',med_med)
            
            word_tfidfs_max_dict = word_tfidfs_max(word_tfidfs)
            word_tfidfs_min_dict = word_tfidfs_min(word_tfidfs)
            #加入总体停用表
            self.all_stop_words = []
            for i in word_tfidfs_max_dict.keys():
                if word_tfidfs_max_dict[i] < med_med:
                    self.all_stop_words.append(i)
                    
            print(stock_name+' stage '+str(self.stage_index),'num of all_stop_words = ',len(self.all_stop_words))
            
            self.all_keep_words = []
            for i in word_tfidfs_min_dict.keys():
                if word_tfidfs_min_dict[i] > med_med:
                    self.all_keep_words.append(i)
                    
            print(stock_name+' stage '+str(self.stage_index),'num of all_keep_words = ',len(self.all_keep_words))
            
            self.all_choose_words = []
            for i in word_tfidfs_min_dict.keys():
                if word_tfidfs_min_dict[i] <= med_med and word_tfidfs_max_dict[i] >= med_med:
                    self.all_choose_words.append(i)
            print(stock_name+' stage '+str(self.stage_index),'num of all_choose_words = ',len(self.all_choose_words))
            
            print(stock_name+' stage '+str(self.stage_index),'filtering tfidf')
        #    filter_result = []
        #    for i in stock_content_tfidf:
        #        words = i[5].split()
        #        med = np.median(list(i[0].values()))
        #        re_words = []
        #        for j in words:
        #            if j in all_stop_words:
        #                continue
        #            elif j in all_keep_words:
        #                re_words.append(j)
        #            elif i[0][j] < med:
        #                continue
        #            else:
        #                re_words.append(j)
        #        filter_result.append(i+[re_words])
            
            
            
            filter_result = pool.map(self.get_filter_result,stock_content_tfidf)
            
            filter_result = [i for i in filter_result if len(i[5].split()) >=10] 
            
            print(stock_name+' stage '+str(self.stage_index),'filter done','news num = ',len(filter_result))
            
                
            self.stage_index += 1
            
            final_result += filter_result
        
        pd.DataFrame(final_result).to_csv(main_path + 'Elara/Documents/paper/tiidf/tfidf_filter/filterresult/'+stock_name+'.csv',encoding='utf-8',header=False,index=False)
        pool.close()


for i in stock_names:
    t = get_filtered_corpus()
    t.get_filtered_corpus(i)