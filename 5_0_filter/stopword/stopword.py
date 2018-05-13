#coding: utf-8

import platform
import os
import struct
import sys
import codecs
import pandas as pd
if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
model_path_3 = main_path+'Elara/Documents/THULAC_pro_c++_v1/models/'


# thulac停用词
xudat_path = model_path_3 + 'xu.dat'
timedat_path = model_path_3 + 'time.dat'

class Dat:

    def __init__(self, filename=None, datSize=None, oldDat=None):
        if(filename):
            try:
                inputfile = open(filename, "rb")
            except:
                print("open file %s failed" % filename)
                sys.exit()
            self.datSize = int(os.path.getsize(filename) / 8)
            
            s = inputfile.read(8 * self.datSize)
            tmp = "<"+str(self.datSize*2)+"i"
            self.dat = struct.unpack(tmp, s)
            self.dat = tuple(self.dat)
            inputfile.close()
        else:
            self.dat = oldDat
            self.dat = tuple(self.dat)
            self.datSize = datSize

xu_dat = Dat(xudat_path).dat
time_dat = Dat(timedat_path).dat

xu_list = list(set([chr(i) for i in xu_dat if i>=0]))
time_list = list(set([chr(i) for i in time_dat if i>=0]))

# 网络停用词 & 叹词
temp = []
for i in os.listdir(main_path+'Elara/Documents/paper/5_0_filter/stopword/stopword/'):
    f = codecs.open(main_path+'Elara/Documents/paper/5_0_filter/stopword/stopword/' + i,encoding='utf-8')
    temp += f.readlines()
    f.close
net_stop_word = [i.strip() for i in temp]

# 用户停用词
user_stopword = ['公司','市场','行业','企业','板块','数据','情况','时间','比例','记者','人士','领域','同比','水平','整体','原因','数量',
              '年度','因素','股份有限公司','过程','事实上','有限责任公司','万元','亿元','千元','万亿','亿万',
              '重点关注','基础上','比重','实际上']

# 只出现一次的词
once_word = pd.read_csv(main_path+'Elara/Documents/paper/4_seg/kv/res/merged_frequency_all.csv',names=['word','cnt'])
once_word = list(once_word.loc[(once_word['cnt']==1),['word']]['word'])

stop_word = list(set(xu_list + time_list + net_stop_word + user_stopword))

f = codecs.open(main_path + 'Elara/Documents/paper/5_0_filter/stopword/stop_word.txt',mode='w',encoding='utf-8')
for i in stop_word:
    f.write(i+'\n')
f.close()