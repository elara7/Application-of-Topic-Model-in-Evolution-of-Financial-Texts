# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 12:27:01 2017

@author: elara
"""
import pandas as pd
import os
import thulac
import platform
import multiprocessing 
import math
import copy

cores=multiprocessing.cpu_count()
print('set cores = ' ,cores)

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
    
model_path_3 = main_path+'Elara/Documents/THULAC_pro_c++_v1/models/'
model_path_2 = main_path+'Elara/Documents/Models_v1_v2/models/'
user_word_path = main_path+'Elara/Documents/wordlib/user_word.txt'

thu = thulac.thulac(user_dict=user_word_path, model_path=model_path_3, T2S=False, seg_only=True, filt=True)
# 读取文本



def get_seg(test):
    try:
        #print('succeed')
        return thu.fast_cut(test,text=True)
    except:
        return None
    
def seg(line):
    try:
        if len(line[4]) == 0 or line[4] == 0 or len(line[1]) == 0 or line[1] == 0:
            print(line[0],line[1],' title or content lenght is 0')
            return line
    except:
        if line[4] == None or line[1] == None:
            print(line[0],line[1],' title or content is None')
            return line
    try:
        title = get_seg(line[1])
        content = get_seg(line[4])
        if title == None:
            print(line[0],line[1],' title seg return None')
        if content == None:
            print(line[0],line[1],' content seg return None')
        line[1] = title
        line[4] = content
        return line 
    except:
        print(line[0],line[1],' error')
        return line

if __name__ ==  '__main__':
    
    

    for i in os.listdir(main_path+'Elara/Documents/paper/corpus/'):
        print('loading',i,' done')
        temp = pd.read_csv(main_path+"Elara/Documents/paper/corpus/"+i,
                                   skiprows=1,
                                   names=['index','title','url','date','content','cate','source','conments','uv'], 
                                   engine='python',
                                   encoding="utf-8").drop_duplicates().fillna(0).drop(['index'],axis=1)
        url_ch = pd.Series([i.split('.')[0].split('/')[-1] for i in temp['url']],name = 'url_ch',index=temp.index)
        name = pd.Series([i.split('.')[0].split('urllist')[1]]*len(temp),name='name',index=temp.index)
        temp = pd.concat([name,temp,url_ch],axis=1)
        
        content = [list(temp.iloc[p,:]) for p in range(len(temp)) if temp.iloc[p,4]!=0]
        
        print('check size')
        cut_content_all = []
        for a in range(len(content)):
            len_i = len(content[a][4])
            if len_i>50000:
                print(a,'size bigger than 50000')
                content_raw = copy.deepcopy(content[a])
                k = math.ceil(len_i/50000)
                l = math.ceil(len_i/k)
                x = 0
                y = x + l
                for j in range(k):
                    if x >= len_i:
                        break
                    if y >= len_i:
                        y = len_i-1
                    cut_content = copy.deepcopy(content_raw)
                    cut_content[4] = content_raw[4][x:y]
                    if j==0:
                        content[a] = copy.deepcopy(cut_content)
                    else:
                        cut_content_all.append(cut_content)
                    x = y
                    y = x + l
                print(a,'resize done')
        content += cut_content_all
                        
        print('load',i,' done')
        pool = multiprocessing.Pool(processes = cores)
        res = pool.map(seg, content)
        pool.close()
        print('seg',i,' done')
        
        result = pd.DataFrame(res)
        result.to_csv(main_path + 'Elara/Documents/paper/seg/output_filter/' + i,encoding='utf-8',header=False,index=False)
        print('save',i,' done')


        
#ttt = pd.read_csv(main_path+'Elara/Documents/paper/seg/urllist上汽集团.csv',engine='python',encoding ='utf-8')
