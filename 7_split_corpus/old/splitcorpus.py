# -*- coding: utf-8 -*-
"""
Created on Thu Dec 14 22:18:42 2017

@author: elara
"""
import platform
import os
import pandas as pd
from datetime import datetime
import itertools
from pandasql import sqldf 
import math
import multiprocessing
pysqldf = lambda q: sqldf(q, globals())

if platform.system() == 'Linux':
    main_path = '/mnt/c/'
if platform.system() == 'Windows':
    main_path = 'C:/'
    
corpus_path = main_path + 'Elara/Documents/paper/filter/corpus/'
split_path = main_path + 'Elara/Documents/paper/cluster/cluster_res/result_raw_split/'

# 载入语料数据和分割信息            
j=1
for i in os.listdir(corpus_path):
    if j == 1:
        corpus = pd.read_csv(corpus_path+i,header=None,
                                names=['name','title','url','date','content','cate','source','conments','uv','url_ch'],
                               engine='python',
                               encoding="utf-8")
    else:
        corpus = corpus.append(pd.read_csv(corpus_path+i,header=None,
                                names=['name','title','url','date','content','cate','source','conments','uv','url_ch'],
                               engine='python',
                               encoding="utf-8"))
    j +=1
print('load corpus')

j=1
for i in os.listdir(split_path):

    temp =  pd.read_csv(split_path+i,usecols=['date','volume','is_split'],
                               engine='python',
                               encoding="utf-8")
    name = pd.Series([i.split('volume')[0]]*len(temp),name='name',index=temp.index)
    cluster_type = pd.Series([i.split('volume_')[1].split('.')[0]]*len(temp),name='cluster_type',index=temp.index)
    temp = pd.concat([name,temp,cluster_type],axis=1)
    if j == 1:
        split_info = temp
    else:
        split_info = split_info.append(temp)
    j +=1

print('load split_info')

# 生成主表
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

print('generate main_table')
# 数据汇总
# 打横，2016和all
#q1 = 'select \
#        name,\
#        date,\
#        volume,\
#        sum(case when cluster_type = "2016" then is_split else 0 end) as is_split_2016,\
#        sum(case when cluster_type = "all" then is_split else 0 end) as is_split_all\
#        from split_info\
#        group by name,date,volume'
#split_info_expand_1 = pysqldf(q1)    

q2 = 'select \
    name,\
    date,\
    volume,\
    case when is_split_2016 = 1 then 1 else 0 end as is_split_2016_1,\
    case when is_split_2016 = 2 then 2 else 0 end as is_split_2016_2,\
    case when is_split_2016 = 3 then 3 else 0 end as is_split_2016_3,\
    case when is_split_all = 1 then 1 else 0 end as is_split_all_1,\
    case when is_split_all = 2 then 2 else 0 end as is_split_all_2,\
    case when is_split_all = 3 then 3 else 0 end as is_split_all_3 \
    from \
    (\
    select \
            name,\
            date,\
            volume,\
            sum(case when cluster_type = "2016" then is_split else 0 end) as is_split_2016,\
            sum(case when cluster_type = "all" then is_split else 0 end) as is_split_all\
            from split_info\
            group by name,date,volume\
    		) a '
split_info_expand = pysqldf(q2)

print('expand split_info')

## 汇总数据
q3 = 'select \
m.name as name,\
m.date as date,\
c.title as title,\
c.url as url,\
c.content as content,\
c.cate as cate,\
c.source as source,\
c.conments as conments,\
c.uv as uv,\
c.url_ch as url_ch,\
s.volume as volume,\
s.is_split_2016_1 as is_split_2016_1,\
s.is_split_2016_2 as is_split_2016_2,\
s.is_split_2016_3 as is_split_2016_3,\
s.is_split_all_1 as is_split_all_1,\
s.is_split_all_2 as is_split_all_2,\
s.is_split_all_3 as is_split_all_3 \
from \
(		\
select \
* \
from main_table \
) m \
left outer join \
( \
select \
*\
from corpus \
) c \
on m.name = c.name and m.date = c.date \
left outer join \
( \
select \
* \
from split_info_expand \
) s \
on m.name = s.name and m.date = s.date \
order by m.name,m.date '

data_all = pysqldf(q3)
print('get data_all')
#df=data_all.loc[data_all['name']=='上汽集团']
## 填补分裂标签
def fill_tag(df):
    df = df.sort_values(by='date')
    a1,a2,a3,n1,n2,n3,f = 1,1,1,1,1,1,1
    #11 12 13  14 15 16
    
    for i in range(len(df)):
        if i == 0:
            da = df.iloc[i,1]
        if i>0:
            if df.iloc[i,1] == da:
                df.iloc[i,11]=a1
                df.iloc[i,12]=a2
                df.iloc[i,13]=a3 
                df.iloc[i,14]=n1
                df.iloc[i,15]=n2
                df.iloc[i,16]=n3
                continue
            else:
                da = df.iloc[i,1]
        if (not math.isnan(df.iloc[i,11])) and df.iloc[i,11] != 0:
            a1+=1
            a2+=1
            a3+=1
            df.iloc[i,11]=a1
            df.iloc[i,12]=a2
            df.iloc[i,13]=a3
        elif (not math.isnan(df.iloc[i,12])) and df.iloc[i,12] != 0:
            a2+=1
            a3+=1
            df.iloc[i,11]=a1
            df.iloc[i,12]=a2
            df.iloc[i,13]=a3
        elif (not math.isnan(df.iloc[i,13]))and df.iloc[i,13] != 0:
            a3+=1
            df.iloc[i,11]=a1
            df.iloc[i,12]=a2
            df.iloc[i,13]=a3
        else:
            df.iloc[i,11]=a1
            df.iloc[i,12]=a2
            df.iloc[i,13]=a3
            
        if (not math.isnan(df.iloc[i,14])) and df.iloc[i,14] != 0:
            n1+=1
            n2+=1
            n3+=1
            df.iloc[i,14]=n1
            df.iloc[i,15]=n2
            df.iloc[i,16]=n3
        elif (not math.isnan(df.iloc[i,15])) and df.iloc[i,15] != 0:
            n2+=1
            n3+=1
            df.iloc[i,14]=n1
            df.iloc[i,15]=n2
            df.iloc[i,16]=n3
        elif (not math.isnan(df.iloc[i,16])) and df.iloc[i,16] != 0:
            n3+=1
            df.iloc[i,14]=n1
            df.iloc[i,15]=n2
            df.iloc[i,16]=n3
        else:
            df.iloc[i,14]=n1
            df.iloc[i,15]=n2
            df.iloc[i,16]=n3
    return df

def applyParallel(dfGrouped, func):
    with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
        ret_list = p.map(func, [group for name, group in dfGrouped])
    return pd.concat(ret_list)

if __name__ == '__main__':
    print('start grouping')
    dfk = applyParallel(data_all.groupby(['name']), fill_tag)
    print('fill tag done')
    print('nrow = ',len(dfk))
    print('ncol = ',len(dfk.columns))
    print('col = ',dfk.columns)
    print('head 10:')
    print(dfk.head(10))
    print('tail 10:')
    print(dfk.tail(10))
    dfk.to_csv(main_path+'Elara/Documents/paper/split_corpus/'+'split_result.csv',encoding='utf-8',header=False,index=False)
    print('save ok')

