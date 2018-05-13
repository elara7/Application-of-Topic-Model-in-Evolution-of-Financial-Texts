# -*- coding: utf-8 -*-
"""
Created on Sat Dec 16 01:07:06 2017

@author: elara
"""


import platform
import pandas as pd
import multiprocessing
import pickle
import numpy as np
cores = multiprocessing.cpu_count()*2
print('set cores = ',cores)


class util_f(object):
    def merge_dict_y2x(self,x,y,mode='plus'):
        # 输入2个dict
        if mode == 'plus':
            for k, v in y.items():
                if k in x.keys():
                    #print(k,' already in x')
                    x[k] += v
                else:
                    x[k] = v
            return x
        elif mode == 'mines':
            for k, v in x.items():
                if k in y.keys():
                    #print(k,' already in x')
                    x[k] -= y[k]
            return x
        else:
            print(mode,' mode error')
            
                
    def merge_dict(self,dict_list):
        # 输入1个list，里面每个元素是dict
        res = {}
        for i in dict_list:
            res = self.merge_dict_y2x(res,i)
        return res
    
    def merge_dftf_bytime(self,dict_variable,f_type,start,end,time_type='in'):
        if time_type == 'in':
            return self.merge_dict([dict_variable[i][f_type] for i in dict_variable if int(i)>=int(start) and int(i)<=int(end)])
        elif time_type == 'out':
            return self.merge_dict([dict_variable[i][f_type] for i in dict_variable if int(i)<int(start) or int(i)>int(end)])
        else:
            print(time_type,' time_type error')
    
    def cal_tfidf(tf,len_doc,df,D):
        return (tf/len_doc)*(D/df)
    
class load_num(object):
    def __init__(self):
        if platform.system() == 'Linux':
            self.main_path = '/mnt/c/'
        #main_path = '/media/elara/56E8C0A9E8C088A7/'
        if platform.system() == 'Windows':
            self.main_path = 'C:/'
        self.num_path = self.main_path + 'Elara/Documents/paper/document_num/'
        self.date_cnt=[]
        self.name_date_cnt=[]
    
    def load_num(self,num_name,init=False):
        if num_name == 'date_cnt':
            if len(self.date_cnt)==0 or init:
                num_file_path = self.num_path + 'date_cnt.csv'
                self.date_cnt = pd.read_csv(num_file_path,names=['date','news_cnt'],engine='python',encoding='utf-8')
        elif num_name == 'name_date_cnt':
            if len(self.name_date_cnt)==0 or init:
                num_file_path = self.num_path + 'name_date_cnt.csv'
                self.name_date_cnt = pd.read_csv(num_file_path,names=['name','date','news_cnt'],engine='python',encoding='utf-8')
        else:
            return None
        

class load_dict(object):
    def __init__(self):
        if platform.system() == 'Linux':
            self.main_path = '/mnt/c/'
        #main_path = '/media/elara/56E8C0A9E8C088A7/'
        if platform.system() == 'Windows':
            self.main_path = 'C:/'
        
        self.dict_path = self.main_path + 'Elara/Documents/paper/tiidf/dict/'
        self.dftf_overall_dict={}
        self.dftf_stock_dict={}
        self.dftf_stock_s_dict={}
    
    def load_file(self,dict_file_path):
        f2 = open(dict_file_path,"rb")
        dict_file = pickle.load(f2)
        f2.close()
        return dict_file
        
    
    def load_dict(self,dict_name,init = False):
        if dict_name == 'dftf_overall_dict':
            if len(self.dftf_overall_dict)==0 or init:
                print('load')
                dict_file_path = self.dict_path + 'overall/dftf_overall_dict.txt'
                self.dftf_overall_dict = self.load_file(dict_file_path)

        elif dict_name == 'dftf_stock_dict':
            if len(self.dftf_stock_dict)==0 or init:
                print('load')
                dict_file_path = self.dict_path + 'dftf_stock_dict.txt'
                self.dftf_stock_dict = self.load_file(dict_file_path)
             
        else:
            if dict_name not in self.dftf_stock_s_dict.keys() or init:
                print('load')
                dict_file_path = self.dict_path + 'stock/'+ dict_name +'_dftf_dict.txt'
                self.dftf_stock_s_dict[dict_name] = self.load_file(dict_file_path)  


    
class stock_keyword(object):
    def __init__(self):
        self._d = load_dict()
        self._num = load_num()
        self.util_f = util_f()
    
    def stock_keyword(self,name,start,end,f_type='content'):
        
        return self.cal_tfdf(name=name,start=start,end=end,f_type=f_type)
        #start到end之间，所有与该股相关的文本中的词进行计算，每个词的tf=该股所有相关文本中该词的tf，df=全局去重,与该股无关的文本中该词的df，
        #len_doc=该股所有相关文本总词数，D=全局去重，与该股无关的文本数
    
    def cal_tfidf_2(self,name,start,end,f_type = 'content'):
        res_tf = self.cal_tf_len_s_t(name = name, start = start, end = end, f_type = f_type)
        for i in res_tf[0].keys():
            res_tf[0][i] = res_tf[0][i]/res_tf[1] 
        
        res_tf = res_tf[0]
        
        res_df = self.cal_df_s_t(name = name, start = start, end = end, f_type = f_type)
        res_N = self.N_s_t(name = name, start = start, end = end)
        
        for i in res_df.keys():
            res_df[i] = res_df[i]/res_N
            
        res_df = self.cal_df_s_t(name = name, start = start, end = end, f_type = f_type)
        res_N = self.N_s_t(name = name, start = start, end = end)
        
        for i in res_df.keys():
            res_df[i] = res_df[i]/res_N
        
    
    def cal_tfidf(self,name,start,end,f_type='content'):
        res_tf = self.cal_tf_len_s_t(name = name, start = start, end = end, f_type = f_type)
        for i in res_tf[0].keys():
            res_tf[0][i] = res_tf[0][i]/res_tf[1] 
        
        res_tf = res_tf[0]
        
        res_df = self.cal_df_s_t(name = name, start = start, end = end, f_type = f_type)
        res_N = self.N_s_t(name = name, start = start, end = end)
        
        for i in res_df.keys():
            res_df[i] = res_df[i]/res_N
            
        tfidf = {}
        for i in res_tf.keys():
            try:
                tf = res_tf[i]
            except:
                tf = 0.00004
            try:
                df = res_df[i]
            except:
                df = 0.00004
            tfidf[i] = tf/df
            
        return tfidf
            
        
        
        
        
    def cal_tf_len_s_t(self,name,start,end,f_type='content',mode='both'):
        if f_type == 'content':
            f_type = 'c_tf'
        if f_type == 'title':
            f_type = 't_tf'
        self._d.load_dict(dict_name = name)
        temp = self.util_f.merge_dftf_bytime(dict_variable = self._d.dftf_stock_s_dict[name],f_type=f_type,start=start,end=end)
        if mode=='both':
            return [temp,sum(temp.values())]
        elif mode == 'len':
            return sum(temp.values())
        elif mode == 'tf':
            return temp
        
    def cal_tf_len_s_ot(self,name,start,end,f_type='content',mode='both'):
        if f_type == 'content':
            f_type = 'c_tf'
        if f_type == 'title':
            f_type = 't_tf'
        self._d.load_dict(dict_name = name)
        temp = self.util_f.merge_dftf_bytime(dict_variable = self._d.dftf_stock_s_dict[name],f_type=f_type,start=start,end=end,time_type='out')
        if mode=='both':
            return [temp,sum(temp.values())]
        elif mode == 'len':
            return sum(temp.values())
        elif mode == 'tf':
            return temp
    
    def cal_tf_len_os_t(self,tf_s_t_res,start,end,f_type='content',mode='both'):
        if f_type == 'content':
            f_type = 'c_tf'
        if f_type == 'title':
            f_type = 't_tf'
        self._d.load_dict('dftf_overall_dict')
        temp_overall = self.util_f.merge_dftf_bytime(dict_variable = self._d.dftf_overall_dict,f_type=f_type,start=start,end=end)
        temp = self.util_f.merge_dict_y2x(x = temp_overall,y = tf_s_t_res,mode='mines')
        if mode=='both':
            return [temp,sum(temp.values())]
        elif mode == 'len':
            return sum(temp.values())
        elif mode == 'tf':
            return temp
    
#######################################################
    def cal_df_s_t(self,name,start,end,f_type='content'):
        if f_type == 'content':
            f_type = 'c_df'
        if f_type == 'title':
            f_type = 't_df'
        self._d.load_dict(dict_name = name)
        return self.util_f.merge_dftf_bytime(dict_variable = self._d.dftf_stock_s_dict[name],f_type=f_type,start=start,end=end)
        

    
    def cal_df_s_ot(self,name,start,end,f_type='content'):
        if f_type == 'content':
            f_type = 'c_df'
        if f_type == 'title':
            f_type = 't_df'
        self._d.load_dict(dict_name = name)
        return self.util_f.merge_dftf_bytime(dict_variable = self._d.dftf_stock_s_dict[name],f_type=f_type,start=start,end=end,time_type='out')
    
    def cal_df_os_t(self,tf_s_t_res,start,end,f_type='content'):
        if f_type == 'content':
            f_type = 'c_df'
        if f_type == 'title':
            f_type = 't_df'
        self._d.load_dict('dftf_overall_dict')
        temp_overall = self.util_f.merge_dftf_bytime(dict_variable = self._d.dftf_overall_dict,f_type=f_type,start=start,end=end)
        return self.util_f.merge_dict_y2x(x = temp_overall,y = tf_s_t_res,mode='mines')


    def N_s_t(self,name,start,end):
        self._num.load_num('name_date_cnt')        
        return sum(self._num.name_date_cnt.loc[(self._num.name_date_cnt['name']==name) & (self._num.name_date_cnt['date']<=int(end)) & (self._num.name_date_cnt['date']>=int(start)),]['news_cnt'])


    def N_s_ot(self,name,start,end):
        self._num.load_num('name_date_cnt')        
        return sum(self._num.name_date_cnt.loc[(self._num.name_date_cnt['name']==name) & ((self._num.name_date_cnt['date']>=int(end)) | (self._num.name_date_cnt['date']<=int(start))),]['news_cnt'])


    def N_os_t(self,N_s_t_res,start,end):
        self._num.load_num('date_cnt')
        total = sum(self._num.date_cnt.loc[(self._num.date_cnt['date']<=int(end)) & (self._num.date_cnt['date']>=int(start)),]['news_cnt'])
        return total - N_s_t_res
    
########################
    def cal_tf_ratio(self,name,start,end,f_type='content'):
        
        res_s_t = self.cal_tf_len_s_t(name=name,start=start,end=end,f_type=f_type)
        
        res_s_ot = self.cal_tf_len_s_ot(name=name,start=start,end=end,f_type=f_type)
        
        res_os_t = self.cal_tf_len_os_t(res_s_t[0],start=start,end=end,f_type=f_type)
        
        for i in res_s_t[0].keys():
            res_s_t[0][i] = res_s_t[0][i]/res_s_t[1]
        
        for i in res_s_ot[0].keys():
            res_s_ot[0][i] = res_s_ot[0][i]/res_s_ot[1]
            
        for i in res_os_t[0].keys():
            res_os_t[0][i] = res_os_t[0][i]/res_os_t[1]
        
        return {'s_t':res_s_t[0], 's_ot':res_s_ot[0], 'os_t':res_os_t[0]}

    def cal_df_ratio(self,name,start,end,f_type='content'):
        res_df_s_t = self.cal_df_s_t(name=name,start=start,end=end,f_type=f_type)
        
        res_df_s_ot = self.cal_df_s_ot(name=name,start=start,end=end,f_type=f_type)
        
        res_df_os_t = self.cal_df_os_t(res_df_s_t,start=start,end=end,f_type=f_type)
        
        res_N_s_t = self.N_s_t(name=name,start=start,end=end)
        
        res_N_s_ot = self.N_s_ot(name=name,start=start,end=end)
        
        res_N_os_t = self.N_os_t(res_N_s_t,start=start,end=end)
        
        for i in res_df_s_t.keys():
            res_df_s_t[i] = res_df_s_t[i]/res_N_s_t
        
        for i in res_df_s_ot.keys():
            res_df_s_ot[i] = res_df_s_ot[i]/res_N_s_ot
            
        for i in res_df_os_t.keys():
            res_df_os_t[i] = res_df_os_t[i]/res_N_os_t
        
        return {'s_t':res_df_s_t, 's_ot':res_df_s_ot, 'os_t':res_df_os_t}

    def query_tfdf(self,dict_file,dict_type,query_word):
        try:
            if dict_file[dict_type][query_word] == 0:
                return 0.00001
            return dict_file[dict_type][query_word]
        except:
            return 0.00001
        
    def cal_tfdf(self,name,start,end,f_type='content',a1=0.8,a2=1.2,a3=1,b1=0.8,b2=1.2,b3=1):
        
        res_tf = self.cal_tf_ratio(name=name,start=start,end=end,f_type=f_type)
        res_df = self.cal_df_ratio(name=name,start=start,end=end,f_type=f_type)
        res = {}
        
        for i in res_tf['s_t'].keys():
            ptf_s_t = self.query_tfdf(res_tf,'s_t',i)
            pdf_s_t = self.query_tfdf(res_df,'s_t',i)
            ptf_s_ot = self.query_tfdf(res_tf,'s_ot',i)
            pdf_s_ot = self.query_tfdf(res_df,'s_ot',i)
            ptf_os_t = self.query_tfdf(res_tf,'os_t',i)
            pdf_os_t = self.query_tfdf(res_df,'os_t',i)
#            if 0 in (ptf_s_t,pdf_s_t,ptf_s_ot,pdf_s_ot,ptf_os_t,pdf_os_t):
#                print(ptf_s_t,pdf_s_t,ptf_s_ot,pdf_s_ot,ptf_os_t,pdf_os_t)
#            if 0 in (ptf_s_ot,pdf_s_ot,ptf_os_t,pdf_os_t):
#                continue
            tfdf_s_t = pow(ptf_s_t,a1)*pow(pdf_s_t,b1)
            tfdf_s_ot = pow(ptf_s_ot,a2)*pow(pdf_s_ot,b2)
            tfdf_os_t = pow(ptf_os_t,a3)*pow(pdf_os_t,b3)
#            if 0 in (tfdf_s_t,tfdf_s_ot,tfdf_os_t):
#                print(tfdf_s_t,tfdf_s_ot,tfdf_os_t)
#            res[i] = tfdf_s_t/(np.log(tfdf_s_ot)*np.log(tfdf_os_t))
            res[i] = tfdf_s_t/(tfdf_s_ot+tfdf_os_t)
        return res
    
test = stock_keyword()
a=test._d.dftf_stock_s_dict['浦发银行']
res = test.stock_keyword('浦发银行',start= 20170801,end=20170901)
res = test.cal_tfidf('浦发银行',start= 20171111,end=20171201)

res = test.cal_tf_ratio('浦发银行',start= 20171111,end=20171201)
res = test.cal_df_ratio('浦发银行',start= 20171111,end=20171201)

ddd = test._num.date_cnt
dfd = test._num.name_date_cnt        

qqq  = ddd.loc[(ddd['date']>=20171001) & (ddd['date']<=20171111),]
qsq = dfd.loc[(dfd['date']>=20171001) & (dfd['date']<=20171111) & (dfd['name']=='浦发银行'),]



        财经文本中词汇的特殊性：
        一个词的重要性有3个层面，1，高频，2，截面独特，3.时序独特
        1.依靠一个时间范围内，与该个股相关的文本中，该词的频率和覆盖率 TF/len *DF/N， 在这个时间范围外，与该个股有关的文本中，该词的频率低，覆盖率低
        2.在这个时间范围内，与该个股无关的文本中，该词的频率低，覆盖率低，而与该股有关的文本中，该词的频率高，覆盖率高 TF/len * DF/N\
        
        研究范围：时间范围t，股票s，t内与s相关的文本中的所有tf>=1的词
        对s和t中每个词：
        tf_s_t：与s相关的文本，时间t内，总tf
        df_s_t:与s相关的文本，时间t内，总df
        tf_s_ot:与s相关文本中，时间t外，总tf
        df_s_ot:与s相关文本中，时间t外，总df
        tf_os_t:与s无关文本中，时间t内，总tf = 时间t内所有去重文本中，tf - tf_s_t
        df_os_t:与s无关文本中，时间t内，总df = 时间t内所有去重文本中，df - df_s_t
        
        
        
        对s和t：
        len_s_t: 与s相关文本，时间t内，总词数
        N_s_t:与s相关文本，时间t内，总文本数
        len_s_ot:与s相关文本，时间t外，总词数
        N_s_ot：与s相关文本，时间t外，总文本数
        len_os_t：与s无关文本，时间t内，总词数
        N_os_t:与s无关文本，时间t内，总文本数
        
        ptf_s_t = tf_s_t/len_s_t
        pdf_s_t = df_s_t/N_s_t
        ptf_s_ot = tf_s_ot/len_s_ot
        pdf_s_ot = df_s_ot/N_s_ot
        ptf_os_t = tf_os_t/len_os_t
        pdf_os_t = df_os_t/N_os_t
        
        tfdf_s_t = pow(ptf_s_t,a1)*pow(pdf_s_t,b1)
        tfdf_s_ot = pow(ptf_s_ot,a2)*pow(pdf_s_ot,b2)
        tfdf_os_t = pow(ptf_os_t,a3)*pow(pdf_os_t,b3)
        
        tfdf = tfdf_s_t/(tfdf_s_ot*tfdf_os_t)
        
        tf_out：s外总tf
        self._d.load_dict(name)
        self._num.load_num('name_date_cnt')
        
    
    

a = pd.Series((True,False,True,False))
b = pd.Series((True,False,False,True))
a && b
