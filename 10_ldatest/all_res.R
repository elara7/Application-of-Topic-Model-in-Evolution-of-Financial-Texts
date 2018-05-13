require(stringr)
require(sqldf)
filenames = dir('C:\\Elara\\Documents\\paper\\10_ldatest\\all_res\\')
for (i in 1:length(filenames))
{
  if (i==1){
    temp = read.csv(str_c('C:\\Elara\\Documents\\paper\\10_ldatest\\all_res\\',filenames[i]),header=FALSE)
  }
  else
  {
    temp = rbind(temp,read.csv(str_c('C:\\Elara\\Documents\\paper\\10_ldatest\\all_res\\',filenames[i]),header=FALSE))
  }
  
}


names(temp) = c('doc_cnt','dict_len','topic_num','is_rep')



t = sqldf('select doc_cnt,dict_len,
          min(topic_num) as topic_num 
          from temp where is_rep=1 group by doc_cnt,dict_len')

boxplot(doc_cnt~topic_num,data=t,xlab='最大无重复主题数',ylab='文章数')

q = quantile(t[,'doc_cnt'])
q[1]=0
t = cbind(t,cut(t[,'doc_cnt'],breaks = q,labels = c(1,2,3,4)))

names(t)[4]='cut'


chisq.test(t[,'cut'],t[,'topic_num'])


t = sqldf('select doc_cnt,dict_len,
          case when doc_cnt<= then 1
          when doc_cnt<=1200 and doc_cnt>=601 then 2
          when doc_cnt<=1800 and doc_cnt>=1201 then 3
          when doc_cnt<=2400 and doc_cnt>=1801 then 4
          when doc_cnt<=3000 and doc_cnt>=2401 then 5
          when doc_cnt<=3600 and doc_cnt>=3001 then 6
          else 7 end as doc_set, 
          min(topic_num) as topic_num 
          from temp where is_rep=1 group by doc_cnt,dict_len')



sqldf('select doc_set ,count(1) from t group by doc_set')
chisq.test(t[,'doc_set'],t[,'topic_num'])

cor(t['doc_cnt'],t['topic_num'])

max(t['doc_cnt'])
min(t['doc_cnt'])

1-600
601-1200
1201-1800
1801-2400
2401-3000
3001-3600
3601-4200


