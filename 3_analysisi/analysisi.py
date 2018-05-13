# -*- coding: utf-8 -*-
"""
Created on Sat Dec  9 10:22:17 2017

@author: elara
"""

import pandas as pd
import os
import math
import numpy as np
from matplotlib import pyplot as plt
from datetime import datetime
import datetime as dt
import itertools
from pandasql import sqldf
import numpy as np
import scipy.stats as stats
from copy import deepcopy
import rpy2
#载入数据(去重)
pysqldf = lambda q: sqldf(q, globals())
    


            
#网页地址分析#################################################################################################################################

#缺失值统计
本文设计的爬虫系统结构如下：
1.获取上证50成分股在东方财富网的网页列表。通过爬虫模拟登陆东方财富网，模拟搜索和翻页行为，解析搜索结果列表中的文章标题、网页链接地址、文章日期，去重后按股票名称将搜索结果分别保存。搜索结果的提取时间从2015年3月到2017年11月。
共获取到334759个网页地址，其中信威集团的网页数最少，仅获取到1545个网页地址，东兴证券、东方证券、中信证券、中国平安、中国银行、光大证券、兴业银行、华泰证券、工商银行、招商证券、方正证券、民生银行、海通证券、贵州茅台的网页数最多，均达到搜索结果上限10000个。
平均每只股票有6695.18个相关网页，中位数为6381个。各个股票获取到的相关网页数分布如下：

根据网页地址可以区分出网页的来源频道，频道分布如下（占比不足1%的14个频道被合并为other类）：

股票频道（stock）的网页数量最多，占到47%，因为股票频道中有大量的市场行情播报短文、财报数据报告等文章，主要内容以结构化数据为主，文章多为行情列举，没有明确主题和外部事件，大多数文章不适合主题建模。此外针对证券类股票，由于股票频道中有许多数据报告文章引用自这些证券公司，因此证券类股票会有大量与其自身股票行情无关的，单纯是因为引用而产生的相关文章。
财经频道（finance）的网页数占比38%，仅次于股票频道文本，这部分文章主要为事件报道、事件解析、业内观点等，文章文字量较多，适合进行主题分析。此外视频频道（video）的内容为视频播放，属于非文本内容。
因此本文排除了股票、视频频道，以这部分地址为下一步的爬取入口。
# 读取网页地址列表 保存为urllist，一个dict，每个values是一个pddataframe
urllist={}
for i in os.listdir('C:\\Elara\\Documents\\paper\\1_corpus_urllist\\'):
    urllist[i] = pd.read_csv("D:\\Elara\\Documents\\paper\\corpus_urllist\\"+i,
                        skiprows =1,
                        names=['index','title','url','date'], 
                        engine='python',
                        encoding="gbk").drop_duplicates().fillna(0).drop(['index'],axis=1)
						
# 统计每只股票的 名称，url数量，标题缺失比例，url缺失比例，日期缺失比例，保存成dataframe，一个股票一行
urllist_analysis=[]
for i in os.listdir('C:\\Elara\\Documents\\paper\\1_corpus_urllist\\'):
    temp = urllist[i]
    urllist_analysis.append([i.split('.')[0].split('urllist')[1],len(temp),
    len([i for i in temp['title'] if i==0])/len(temp),
    len([i for i in temp['url'] if i==0])/len(temp),
    len([i for i in temp['date'] if i==0])/len(temp)])

urllist_analysis = pd.DataFrame(urllist_analysis)
urllist_analysis.columns=['股票名称','网页数','网页标题缺失率','网页地址缺失率','网页日期缺失率']
urllist_analysis = urllist_analysis.sort_values(by = '网页数',ascending=False)

# 个股统计
sum(urllist_analysis['网页数']) #网页总数
np.mean(urllist_analysis['网页数']) #平均每只个股的网页数
np.median(urllist_analysis['网页数']) #每只个股网页数的中位数
[urllist_analysis['股票名称'][i] for i in range(len(urllist_analysis)) if urllist_analysis['网页数'][i] == min(urllist_analysis['网页数'])] #网页数最少个股
[urllist_analysis['股票名称'][i] for i in range(len(urllist_analysis)) if urllist_analysis['网页数'][i] == max(urllist_analysis['网页数'])] #网页数最多个股


# 频道分析
ch_analysis=[] 
temp = [] #把urllist的所有网址信息拍平
t=1
for i in os.listdir('C:\\Elara\\Documents\\paper\\1_corpus_urllist\\'):
    if t==1:
        temp = urllist[i]
        t += 1
        continue
    else:
        temp = temp.append(urllist[i])

ch_analysis = [i.split('.')[0].split('/')[-1] for i in temp['url']] #解析所有的ch出来
ch_set = set(ch_analysis) #去重得到ch种类
ch_freq={}
for i in ch_analysis:
    try:
        ch_freq[i] += 1
    except:
        ch_freq[i] = 0
ch_freq_df = pd.DataFrame([[i,ch_freq[i]] for i in ch_freq]) # 统计每种ch有多少次


2.以上一步提取的结果列表为爬取入口，多进程并行获取网页内容。去重共获得177633条记录。其中各频道失败率如下：
mss、sec、life、toujiao、pmetal的网页地址均无法获取，人工检查后发现多为已下线页面。彩票频道（caipiao）的失败率达到12%。但这部分频道的网页数量仅占所有待爬取网页的0.23%，并且彩票频道的内容相对来说与研究目标较不一致，因此将这些频道排除。主要建模语料来源财经频道（finance）的失败率仅0.6%，全体失败率仅0.75%，可以满足建模需求。
排除掉上述频道和获取失败的网页后，最终共获得176272条记录,
总体上的评论数分布如下：
呈现明显的长尾分布，符合互联网产品的特征。
其中财经频道（finance）占比71.65%，
且其有评论文章占46.36%，有评论文章中平均评论数为58个，均为各频道最高。综合以上指标考虑,财经频道是一个数据源丰富、覆盖用户量较大的频道，因此本文选择财经频道的文本进行分析建模。
最终的语料集中有126307篇文本，每只股票对应的相关文章数如下：

#网页内容分析############################################################################################################################################
# 读取爬取的内容数据
content=[]
t=1
for i in os.listdir('C:\\Elara\\Documents\\paper\\corpus\\'):
    temp = pd.read_csv("C:\\Elara\\Documents\\paper\\corpus\\"+i,
                               skiprows=1,
                               names=['index','title','url','date','content','cate','source','conments','uv'], 
                               engine='python',
                               encoding="utf-8").drop_duplicates().fillna(0).drop(['index'],axis=1)
    url_ch = pd.Series([i.split('.')[0].split('/')[-1] for i in temp['url']],name = 'url_ch',index=temp.index)
    name = pd.Series([i.split('.')[0].split('urllist')[1]]*len(temp),name='name',index=temp.index)
    temp = pd.concat([name,temp,url_ch],axis=1)
    if t==1:
        content = temp
    else:
        content = content.append(temp)
    t += 1





pysqldf = lambda q: sqldf(q, globals())

#每个频道的缺失率
q = 'select sum(case when a.content = 0 then 1 else 0 end) as bbb ,count(1) as aaa, url_ch from content a group by url_ch'
#剔除指定频道和有丢失的内容以后，每只股票的文章数
q = "select name,count(1) from content a where a.content != 0 and a.url_ch not in ('mss','sec','life','toujiao','pmetal','caipiao') group by name"
#总体评论数据
q = "select conments from content a where a.content != 0 and a.url_ch not in ('mss','sec','life','toujiao','pmetal','caipiao')"  
temp_content = pysqldf(q)
#总体评论数分布
plt.hist(temp_content['conments']  ,bins = np.arange(0, 1000, 10))
#总体浏览数
q = "select uv from content a where a.content != 0 and a.url_ch not in ('mss','sec','life','toujiao','pmetal','caipiao')"
temp_content = pysqldf(q)
#总体浏览数分布
plt.hist(temp_content['uv']  ,bins = np.arange(0, 1000, 10))
#文章的浏览人数、评论人数
q = "select url_ch, sum(case when conments >0 then 1 else 0 end) as conment_page ,sum(case when uv >0 then 1 else 0 end) as uv_page,count(1) as all_page from content a where a.content != 0 and a.url_ch not in ('mss','sec','life','toujiao','pmetal','caipiao') group by url_ch"
#平均、中位参与讨论数 评论数
q = "select * from content a where a.content != 0 and a.url_ch not in ('mss','sec','life','toujiao','pmetal','caipiao') and conments>0"
temp_content = pysqldf(q)
res = temp_content.groupby(['url_ch'])['conments'].median()
q = "select * from content a where a.content != 0 and a.url_ch not in ('mss','sec','life','toujiao','pmetal','caipiao') and uv>0"
temp_content = pysqldf(q)
res = temp_content.groupby(['url_ch'])['uv'].median()

q = "select * from content a where a.content != 0 and a.url_ch not in ('mss','sec','life','toujiao','pmetal','caipiao') and conments>0"
temp_content = pysqldf(q)
res = temp_content.groupby(['url_ch'])['conments'].mean()
q = "select * from content a where a.content != 0 and a.url_ch not in ('mss','sec','life','toujiao','pmetal','caipiao') and uv>0"
temp_content = pysqldf(q)
res = temp_content.groupby(['url_ch'])['uv'].mean()

#限定财经频道以后，每只股票的文章数
q = "select name,count(1) from content a where a.content != 0 and a.url_ch = 'finance' group by name"
res = pysqldf(q)
#限定财经频道以后，总文本
q = "select count(1) from content a where a.content != 0 and a.url_ch = 'finance'"
pysqldf(q)
q="select * from content where content != 0 and url_ch not in ('mss','sec','life','toujiao','pmetal','caipiao') and url_ch = 'fund' limit 10"
res = pysqldf(q)

3.聚类分析
1.全体文章，周级别就有比较明显的相关性，日级别稍微弱一点，一些证券银行类相关性比较低，可以再看到单周，单月，单年的情况
2.财经文本文章，相关性明显比全体文章小。但是单独看stock，相关性也不大。应该是2个一起作用提高了相关性
3.周相关明显高，抹除了噪音，使用全体周数据聚类，同时还可以减少数据量，降低最优分割聚类的计算消耗
#基础数据
urllist=[]
t=1
for i in os.listdir('C:\\Elara\\Documents\\paper\\corpus_urllist\\'):
    temp = pd.read_csv("C:\\Elara\\Documents\\paper\\corpus_urllist\\"+i,
                               skiprows=1,
                               names=['index','title','url','date'], 
                               engine='python',
                               encoding="gbk").drop_duplicates().fillna(0).drop(['index'],axis=1)
    url_ch = pd.Series([i.split('.')[0].split('/')[-1] for i in temp['url']],name = 'url_ch',index=temp.index)
    name = pd.Series([i.split('.')[0].split('urllist')[1]]*len(temp),name='name',index=temp.index)
    temp = pd.concat([name,temp,url_ch],axis=1)
    if t==1:
        urllist = temp
    else:
        urllist = urllist.append(temp)
    t += 1
    
price_volume = pd.read_csv('C:\\Elara\\Documents\\paper\\\market\\price_volume.csv')
for i in range(len(price_volume)):
    price_volume.iloc[i,0] = ''.join(str(datetime.strptime(price_volume.iloc[i,0], '%Y/%m/%d')).split(' ')[0].split('-'))


shares = pd.read_csv('C:\\Elara\\Documents\\paper\\\market\\shares.csv')
for i in range(len(shares)):
    shares.iloc[i,0] = ''.join(str(datetime.strptime(shares.iloc[i,0], '%Y/%m/%d')).split(' ')[0].split('-'))


turnover = pd.read_csv('C:\\Elara\\Documents\\paper\\\market\\turnover.csv')
for i in range(len(turnover)):
    turnover.iloc[i,0] = ''.join(str(datetime.strptime(turnover.iloc[i,0], '%Y/%m/%d')).split(' ')[0].split('-'))



def datelist(beginDate, endDate):
    # beginDate, endDate是形如‘20160601’的字符串或datetime格式
    date_l=[datetime.strftime(x,'%Y%m%d') for x in list(pd.date_range(start=beginDate, end=endDate))]
    return date_l

timelist = datelist('20150101','20171208')        

a = '浦发银行 (600000)	民生银行 (600016)	中国石化 (600028) 南方航空 (600029)	中信证券 (600030)	招商银行 (600036) 保利地产 (600048)	中国联通 (600050)	同方股份 (600100) 上汽集团 (600104)	北方稀土 (600111)	华夏幸福 (600340) 信威集团 (600485)	康美药业 (600518)	贵州茅台 (600519) 山东黄金 (600547)	绿地控股 (600606)	海通证券 (600837) 伊利股份 (600887)	江苏银行 (600919)	东方证券 (600958) 招商证券 (600999)	大秦铁路 (601006)	中国神华 (601088) 兴业银行 (601166)	北京银行 (601169)	中国铁建 (601186) 东兴证券 (601198)	国泰君安 (601211)	上海银行 (601229) 农业银行 (601288)	中国平安 (601318)	交通银行 (601328) 新华保险 (601336)	中国中铁 (601390)	工商银行 (601398) 中国太保 (601601)	中国人寿 (601628)	中国建筑 (601668) 华泰证券 (601688)	中国中车 (601766)	光大证券 (601788) 中国交建 (601800)	光大银行 (601818)	中国石油 (601857) 中国银河 (601881)	方正证券 (601901)	中国核电 (601985) 中国银行 (601988)	中国重工 (601989)	'
a=a.split()    
name_set = [a[i] for i in range(len(a)) if i%2 == 0]
 
main_table = pd.DataFrame([[x[0],x[1]] for x in itertools.product(name_set,timelist)])
main_table.columns = ['name','date']



# 计算每个股票每一天的文章数
#q = 'select name,date,count(1) as num from urllist group by name, date'
#res = pysqldf(q)
cnt = urllist.groupby(['name','date'])['url'].count()
cnt = pd.DataFrame([[cnt.index[i][0],cnt.index[i][1],cnt[i]] for i in range(len(cnt))])
cnt.columns = ['name','date','num']

# 全体文章


q = "select  m.name as name, m.date as date, c.num as num, p.open as open, p.close as close, p.high as high, p.low as low, p.total_turnover as total_turnover, p.volume as volume, p.limit_up as limit_up, p.limit_down as limit_down, s.circulation_a as circulation_a, s.management_circulation as management_circulation, s.non_circulation_a as non_circulation_a, s.total_a as total_a, s.total as s_total, t.today as today, t.week as week, t.month as month, t.three_month as three_month, t.six_month as six_month, t.year as year, t.current_year as current_year, t.total as t_total from (select name, date from main_table) m left outer join (select * from cnt) c  on m.name = c.name and m.date = c.date left outer join (select * from price_volume) p on m.name = p.name and m.date = p.date left outer join (select * from shares) s on m.name = s.name and m.date = s.date left outer join (select * from turnover) t  on m.name = t.name and m.date = t.date"
res = pysqldf(q)

#res.to_csv('C:\\Elara\\Documents\\paper\\analysisi\\summary.csv')

q = 'select * from res where num is not null and volume is not null'
res_no_na = pysqldf(q)

res_no_na.to_csv('C:\\Elara\\Documents\\paper\\analysisi\\summary_nona.csv')

corr_ = []
for i in set(res_no_na['name']):
    q = 'select volume, num from res_no_na where name = "' + i +'"'
    temp = pysqldf(q)
    corr_.append([i] + list(stats.pearsonr(temp['volume'],temp['num'])))


# 构造周级别的数据，日数据噪音太多
def get_monday(date):
    days = 1 - datetime.strptime(date,'%Y%m%d').weekday()
    return ''.join(str(datetime.strptime(date,'%Y%m%d') + dt.timedelta(days = days)).split(' ')[0].split('-'))

res_1w_pre = deepcopy(res)
for i in range(len(res)):
    res_1w_pre.iloc[i,1] = get_monday(res_1w_pre.iloc[i,1])

q = 'select * from (select date,name,sum(volume) as volume, sum(num) as num from res_1w_pre group by date,name) a where num is not null and volume is not null'
res_1w = pysqldf(q)
res_1w.to_csv('C:\\Elara\\Documents\\paper\\analysisi\\summary_1w.csv')
corr_1w = []
for i in set(res_1w['name']):
    q = 'select volume, num from res_1w where name = "' + i +'"'
    temp = pysqldf(q)
    corr_1w.append([i] + list(stats.pearsonr(temp['volume'],temp['num'])))


# 财经频道

q = 'select * from urllist where url_ch = "finance"'
urllist_f = pysqldf(q)
# 计算每个股票每一天的文章数
#q = 'select name,date,count(1) as num from urllist group by name, date'
#res = pysqldf(q)
cnt = urllist_f.groupby(['name','date'])['url'].count()
cnt = pd.DataFrame([[cnt.index[i][0],cnt.index[i][1],cnt[i]] for i in range(len(cnt))])
cnt.columns = ['name','date','num']



q = "select  m.name as name, m.date as date, c.num as num, p.open as open, p.close as close, p.high as high, p.low as low, p.total_turnover as total_turnover, p.volume as volume, p.limit_up as limit_up, p.limit_down as limit_down, s.circulation_a as circulation_a, s.management_circulation as management_circulation, s.non_circulation_a as non_circulation_a, s.total_a as total_a, s.total as s_total, t.today as today, t.week as week, t.month as month, t.three_month as three_month, t.six_month as six_month, t.year as year, t.current_year as current_year, t.total as t_total from (select name, date from main_table) m left outer join (select * from cnt) c  on m.name = c.name and m.date = c.date left outer join (select * from price_volume) p on m.name = p.name and m.date = p.date left outer join (select * from shares) s on m.name = s.name and m.date = s.date left outer join (select * from turnover) t  on m.name = t.name and m.date = t.date"
res = pysqldf(q)

res.to_csv('C:\\Elara\\Documents\\paper\\analysisi\\summary.csv')

q = 'select * from res where num is not null and volume is not null'
res_no_na = pysqldf(q)

res_no_na.to_csv('C:\\Elara\\Documents\\paper\\analysisi\\summary_nona.csv')

corr_ = []
for i in set(res_no_na['name']):
    q = 'select volume, num from res_no_na where name = "' + i +'"'
    temp = pysqldf(q)
    corr_.append([i] + list(stats.pearsonr(temp['volume'],temp['num'])))


# 构造周级别的数据，日数据噪音太多
def get_monday(date):
    days = 1 - datetime.strptime(date,'%Y%m%d').weekday()
    return ''.join(str(datetime.strptime(date,'%Y%m%d') + dt.timedelta(days = days)).split(' ')[0].split('-'))

res_1w_pre = deepcopy(res)
for i in range(len(res)):
    res_1w_pre.iloc[i,1] = get_monday(res_1w_pre.iloc[i,1])

q = 'select * from (select date,name,sum(volume) as volume, sum(num) as num from res_1w_pre group by date,name) a where num is not null and volume is not null'
res_1w = pysqldf(q)
res_1w.to_csv('C:\\Elara\\Documents\\paper\\analysisi\\summary_1w.csv')
corr_1w = []
for i in set(res_1w['name']):
    q = 'select volume, num from res_1w where name = "' + i +'"'
    temp = pysqldf(q)
    corr_1w.append([i] + list(stats.pearsonr(temp['volume'],temp['num'])))

# 股票频道

q = 'select * from urllist where url_ch = "stock"'
urllist_s = pysqldf(q)
# 计算每个股票每一天的文章数
#q = 'select name,date,count(1) as num from urllist group by name, date'
#res = pysqldf(q)
cnt = urllist_f.groupby(['name','date'])['url'].count()
cnt = pd.DataFrame([[cnt.index[i][0],cnt.index[i][1],cnt[i]] for i in range(len(cnt))])
cnt.columns = ['name','date','num']



q = "select  m.name as name, m.date as date, c.num as num, p.open as open, p.close as close, p.high as high, p.low as low, p.total_turnover as total_turnover, p.volume as volume, p.limit_up as limit_up, p.limit_down as limit_down, s.circulation_a as circulation_a, s.management_circulation as management_circulation, s.non_circulation_a as non_circulation_a, s.total_a as total_a, s.total as s_total, t.today as today, t.week as week, t.month as month, t.three_month as three_month, t.six_month as six_month, t.year as year, t.current_year as current_year, t.total as t_total from (select name, date from main_table) m left outer join (select * from cnt) c  on m.name = c.name and m.date = c.date left outer join (select * from price_volume) p on m.name = p.name and m.date = p.date left outer join (select * from shares) s on m.name = s.name and m.date = s.date left outer join (select * from turnover) t  on m.name = t.name and m.date = t.date"
res = pysqldf(q)

#res.to_csv('C:\\Elara\\Documents\\paper\\analysisi\\summary.csv')

q = 'select * from res where num is not null and volume is not null'
res_no_na = pysqldf(q)

#res_no_na.to_csv('C:\\Elara\\Documents\\paper\\analysisi\\summary_nona.csv')

corr_ = []
for i in set(res_no_na['name']):
    q = 'select volume, num from res_no_na where name = "' + i +'"'
    temp = pysqldf(q)
    corr_.append([i] + list(stats.pearsonr(temp['volume'],temp['num'])))


# 构造周级别的数据，日数据噪音太多
def get_monday(date):
    days = 1 - datetime.strptime(date,'%Y%m%d').weekday()
    return ''.join(str(datetime.strptime(date,'%Y%m%d') + dt.timedelta(days = days)).split(' ')[0].split('-'))

res_1w_pre = deepcopy(res)
for i in range(len(res)):
    res_1w_pre.iloc[i,1] = get_monday(res_1w_pre.iloc[i,1])

q = 'select * from (select date,name,sum(volume) as volume, sum(num) as num from res_1w_pre group by date,name) a where num is not null and volume is not null'
res_1w = pysqldf(q)
#res_1w.to_csv('C:\\Elara\\Documents\\paper\\analysisi\\summary_1w.csv')
corr_1w = []
for i in set(res_1w['name']):
    q = 'select volume, num from res_1w where name = "' + i +'"'
    temp = pysqldf(q)
    corr_1w.append([i] + list(stats.pearsonr(temp['volume'],temp['num'])))

4.划分实施

#基础数据
urllist=[]
t=1
for i in os.listdir('C:\\Elara\\Documents\\paper\\corpus_urllist\\'):
    temp = pd.read_csv("C:\\Elara\\Documents\\paper\\corpus_urllist\\"+i,
                               skiprows=1,
                               names=['index','title','url','date'], 
                               engine='python',
                               encoding="gbk").drop_duplicates().fillna(0).drop(['index'],axis=1)
    url_ch = pd.Series([i.split('.')[0].split('/')[-1] for i in temp['url']],name = 'url_ch',index=temp.index)
    name = pd.Series([i.split('.')[0].split('urllist')[1]]*len(temp),name='name',index=temp.index)
    temp = pd.concat([name,temp,url_ch],axis=1)
    if t==1:
        urllist = temp
    else:
        urllist = urllist.append(temp)
    t += 1
    
price_volume = pd.read_csv('C:\\Elara\\Documents\\paper\\\market\\price_volume.csv')
for i in range(len(price_volume)):
    price_volume.iloc[i,0] = ''.join(str(datetime.strptime(price_volume.iloc[i,0], '%Y/%m/%d')).split(' ')[0].split('-'))


shares = pd.read_csv('C:\\Elara\\Documents\\paper\\\market\\shares.csv')
for i in range(len(shares)):
    shares.iloc[i,0] = ''.join(str(datetime.strptime(shares.iloc[i,0], '%Y/%m/%d')).split(' ')[0].split('-'))


turnover = pd.read_csv('C:\\Elara\\Documents\\paper\\\market\\turnover.csv')
for i in range(len(turnover)):
    turnover.iloc[i,0] = ''.join(str(datetime.strptime(turnover.iloc[i,0], '%Y/%m/%d')).split(' ')[0].split('-'))



def datelist(beginDate, endDate):
    # beginDate, endDate是形如‘20160601’的字符串或datetime格式
    date_l=[datetime.strftime(x,'%Y%m%d') for x in list(pd.date_range(start=beginDate, end=endDate))]
    return date_l

timelist = datelist('20150101','20171208')        

a = '浦发银行 (600000)	民生银行 (600016)	中国石化 (600028) 南方航空 (600029)	中信证券 (600030)	招商银行 (600036) 保利地产 (600048)	中国联通 (600050)	同方股份 (600100) 上汽集团 (600104)	北方稀土 (600111)	华夏幸福 (600340) 信威集团 (600485)	康美药业 (600518)	贵州茅台 (600519) 山东黄金 (600547)	绿地控股 (600606)	海通证券 (600837) 伊利股份 (600887)	江苏银行 (600919)	东方证券 (600958) 招商证券 (600999)	大秦铁路 (601006)	中国神华 (601088) 兴业银行 (601166)	北京银行 (601169)	中国铁建 (601186) 东兴证券 (601198)	国泰君安 (601211)	上海银行 (601229) 农业银行 (601288)	中国平安 (601318)	交通银行 (601328) 新华保险 (601336)	中国中铁 (601390)	工商银行 (601398) 中国太保 (601601)	中国人寿 (601628)	中国建筑 (601668) 华泰证券 (601688)	中国中车 (601766)	光大证券 (601788) 中国交建 (601800)	光大银行 (601818)	中国石油 (601857) 中国银河 (601881)	方正证券 (601901)	中国核电 (601985) 中国银行 (601988)	中国重工 (601989)	'
a=a.split()    
name_set = [a[i] for i in range(len(a)) if i%2 == 0]
 
main_table = pd.DataFrame([[x[0],x[1]] for x in itertools.product(name_set,timelist)])
main_table.columns = ['name','date']



# 计算每个股票每一天的文章数
#q = 'select name,date,count(1) as num from urllist group by name, date'
#res = pysqldf(q)
cnt = urllist.groupby(['name','date'])['url'].count()
cnt = pd.DataFrame([[cnt.index[i][0],cnt.index[i][1],cnt[i]] for i in range(len(cnt))])
cnt.columns = ['name','date','num']

#删掉周六周日
def datelist(beginDate, endDate):
    # beginDate, endDate是形如‘20160601’的字符串或datetime格式
    date_l=[datetime.strftime(x,'%Y%m%d') for x in list(pd.date_range(start=beginDate, end=endDate))]
    return date_l

timelist = datelist('20150101','20171208')        

timelist = [i for i in timelist if datetime.strptime(i,'%Y%m%d').weekday() not in (5,6)]
    

a = '浦发银行 (600000)	民生银行 (600016)	中国石化 (600028) 南方航空 (600029)	中信证券 (600030)	招商银行 (600036) 保利地产 (600048)	中国联通 (600050)	同方股份 (600100) 上汽集团 (600104)	北方稀土 (600111)	华夏幸福 (600340) 信威集团 (600485)	康美药业 (600518)	贵州茅台 (600519) 山东黄金 (600547)	绿地控股 (600606)	海通证券 (600837) 伊利股份 (600887)	江苏银行 (600919)	东方证券 (600958) 招商证券 (600999)	大秦铁路 (601006)	中国神华 (601088) 兴业银行 (601166)	北京银行 (601169)	中国铁建 (601186) 东兴证券 (601198)	国泰君安 (601211)	上海银行 (601229) 农业银行 (601288)	中国平安 (601318)	交通银行 (601328) 新华保险 (601336)	中国中铁 (601390)	工商银行 (601398) 中国太保 (601601)	中国人寿 (601628)	中国建筑 (601668) 华泰证券 (601688)	中国中车 (601766)	光大证券 (601788) 中国交建 (601800)	光大银行 (601818)	中国石油 (601857) 中国银河 (601881)	方正证券 (601901)	中国核电 (601985) 中国银行 (601988)	中国重工 (601989)	'
a=a.split()    
name_set = [a[i] for i in range(len(a)) if i%2 == 0]
 
main_table = pd.DataFrame([[x[0],x[1]] for x in itertools.product(name_set,timelist)])
main_table.columns = ['name','date']

#汇总
q = "select  m.name as name, m.date as date, c.num as num, p.open as open, p.close as close, p.high as high, p.low as low, (case when p.total_turnover = 0 then null else p.total_turnover end) as total_turnover, (case when p.volume = 0 then null else p.volume end) as volume, p.limit_up as limit_up, p.limit_down as limit_down, s.circulation_a as circulation_a, s.management_circulation as management_circulation, s.non_circulation_a as non_circulation_a, s.total_a as total_a, s.total as s_total, t.today as today, t.week as week, t.month as month, t.three_month as three_month, t.six_month as six_month, t.year as year, t.current_year as current_year, t.total as t_total from (select name, date from main_table) m left outer join (select * from cnt) c  on m.name = c.name and m.date = c.date left outer join (select * from price_volume) p on m.name = p.name and m.date = p.date left outer join (select * from shares) s on m.name = s.name and m.date = s.date left outer join (select * from turnover) t  on m.name = t.name and m.date = t.date"
res_noweek = pysqldf(q)
res_noweek.to_csv('C:\\Elara\\Documents\\paper\\analysisi\\res_noweek.csv')
##去掉非交易日和停牌期间
#q = 'select * from res where num is not null and volume is not null and volume > 0'
#res_no_na = pysqldf(q)
#
#res_no_na.to_csv('C:\\Elara\\Documents\\paper\\analysisi\\summary_nona.csv')


观察发现，2015年9月大跌后，交易量大减，显然15年的一波要分开分析，这次只考虑16年后的情况，观察发现大约2016年2月底3月初，9月大跌触底反弹后的复跌到底，之后明显是一个缓步爬升的稳定状态，因此考虑的时间 范围是20160301-20171130
划分思路：
将（20160229之后）交易量的取值范围划分成100等分，计算hist
选择频率最高的范围作为基准范围
将高于、低于、等于基准范围的每个连续区间作为一个窗口

#去掉缺失值

x = pd.Series(res_noweek.loc[(res_noweek['name']=='浦发银行') & (res_noweek['date']>=str(20160229))]['volume'].dropna())
#计算bins
bins_num = 100
bins = [min(x)]
bin_width = (max(x) - min(x))/bins_num
for i in range(bins_num):
    bins.append(bins[-1]+bin_width)
#原始数据添加bins标签
x2 = res_noweek.loc[(res_noweek['name']=='浦发银行'),('name','date','volume')]
x1 = []
for i in range(len(x2)):
    temp = res_noweek.loc[i,'volume']
    if np.isnan(temp):
        x1.append(list(res_noweek.loc[i,['name','date','volume']]) + [float('nan')])
    else:
        for j in range(len(bins)-1):
            if temp >= bins[j] and temp < bins[j+1]:
                x1.append(list(res_noweek.loc[i,['name','date','volume']]) + [j])
            else:
                continue
x1 = pd.DataFrame(x1)
x1.columns = ['name','date','volume','cate']
catecnt = x1['cate'].value_counts().sort_index()   
plt.plot(catecnt.index, catecnt )    
plt.plot(x1['volume'])    

x_ = pd.read_csv('C:\\Elara\\Documents\\paper\\analysisi\\stlfrom20150101\\贵州茅台.csv',engine='python')
x = pd.Series(x_.loc[(x_['name']=='贵州茅台') & (x_['date']>=20160229)]['fc.21'].dropna())
#计算bins
bins_num = 6
bins = [min(x)]
bin_width = (max(x) - min(x))/bins_num
for i in range(bins_num):
    bins.append(bins[-1]+bin_width)
#原始数据添加bins标签
x2 = x_.loc[(x_['name']=='贵州茅台') & (x_['date']>=20160229),('name','date','fc.21')]
x1 = []
for i in range(len(x2)):
    temp = x2.iloc[i,]['fc.21']
    if np.isnan(temp):
        x1.append(list(x2.iloc[i,][['name','date','fc.21']]) + [float('nan')])
    else:
        for j in range(len(bins)-1):
            if temp >= bins[j] and temp < bins[j+1]:
                x1.append(list(x2.iloc[i,][['name','date','fc.21']]) + [j])
            else:
                continue
        if temp >= max(bins):
            x1.append(list(x2.iloc[i,][['name','date','fc.21']]) + [bins_num])
            
x1 = pd.DataFrame(x1)
x1.columns = ['name','date','fc.21','cate']
catecnt = x1['cate'].value_counts().sort_index()   
plt.plot(catecnt.index, catecnt )    
plt.plot(x1['volume'])  


import pickle
f2 = open('C:/Elara/Documents/paper/analysisi/all_frequency_dict.txt',"rb")
all_frequency_dict = pickle.load(f2)
f2.close()
word_frequency = all_frequency_dict.values()
from matplotlib import pyplot as plt
plt.hist(word_frequency,bins=range(0,40000,1000))
plt.hist(word_frequency,bins=range(0,4000,100))
plt.hist(word_frequency,bins=range(0,400,10))
plt.hist(word_frequency,bins=range(0,40,1))
# 一次和2次的非常多

