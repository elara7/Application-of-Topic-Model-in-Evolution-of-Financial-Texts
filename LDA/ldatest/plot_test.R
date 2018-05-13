setwd('C:/Elara/Documents/paper/LDA')
library(stringr)
a = read.csv('426lda_test_res_test_set.csv',header = FALSE)
names(a)=c('训练文档数','测试文档数','字典长度','主题数','迭代次数','是否重复',1:100)

df=a


get_test_res <- function(df)
{
  
  i500 = df[df['迭代次数']==500 & df['主题数']!=200 & df['主题数']!=100 & df['主题数']!=50,]
  i1000 = df[df['迭代次数']==1000,]
  i2000 = df[df['迭代次数']==2000,]
  x = 1:100
  x_s = seq(1,100,10)
  
  
  file_name = str_c('C:/Elara/Documents/paper/LDA/all_pic/',df[1,'训练文档数'],'_',500,'.png')
  png(file_name,res = 100,width = 700,height = 400)
  par(mar = c(4, 4,3, 0.7))
  max_y = max(i500[,7:ncol(i500)])
  min_y = min(i500[,7:ncol(i500)])
  plot(x,i500[1,6:ncol(test)],type='l',ylim=c(min_y,max_y),xlim = c(1,120),xlab='遍历次数',ylab = '困惑度')
  points(x_s,i500[1,6:ncol(test)][x_s],pch=0,cex=0.7)
  lines(x,i500[2,6:ncol(test)],type='l')
  points(x_s,i500[2,6:ncol(test)][x_s],pch=1,cex=0.7)
  lines(x,i500[3,6:ncol(test)],type='l')
  points(x_s,i500[3,6:ncol(test)][x_s],pch=2,cex=0.7)
  lines(x,i500[4,6:ncol(test)],type='l')
  points(x_s,i500[4,6:ncol(test)][x_s],pch=3,cex=0.7)
  lines(x,i500[5,6:ncol(test)],type='l')
  points(x_s,i500[5,6:ncol(test)][x_s],pch=4,cex=0.7)
  legend("topright", pch=0:4,title = '主题数',
         legend=c(5,20,50,100,200))
  dev.off()
  
  file_name = str_c('C:/Elara/Documents/paper/LDA/all_pic/',df[1,'文档数'],'_',1000,'.png')
  png(file_name,res = 100,width = 700,height = 400)
  par(mar = c(4, 4,3, 0.7))
  plot(x,i1000[1,6:ncol(test)],type='l',ylim=c(min_y,max_y),xlim = c(1,120),xlab='遍历次数',ylab = '困惑度')
  points(x_s,i1000[1,6:ncol(test)][x_s],pch=0,cex=0.7)
  lines(x,i1000[2,6:ncol(test)],type='l')
  points(x_s,i1000[2,6:ncol(test)][x_s],pch=1,cex=0.7)
  lines(x,i1000[3,6:ncol(test)],type='l')
  points(x_s,i1000[3,6:ncol(test)][x_s],pch=2,cex=0.7)
  lines(x,i1000[4,6:ncol(test)],type='l')
  points(x_s,i1000[4,6:ncol(test)][x_s],pch=3,cex=0.7)
  lines(x,i1000[5,6:ncol(test)],type='l')
  points(x_s,i1000[5,6:ncol(test)][x_s],pch=4,cex=0.7)
  legend("topright", pch=0:4,title = '主题数',
         legend=c(5,20,50,100,200))
  dev.off()
  
  file_name = str_c('C:/Elara/Documents/paper/LDA/all_pic/',df[1,'文档数'],'_',2000,'.png')
  png(file_name,res = 100,width = 700,height = 400)
  par(mar = c(4, 4,3, 0.7))
  plot(x,i2000[1,6:ncol(test)],type='l',ylim=c(min_y,max_y),xlim = c(1,120),xlab='遍历次数',ylab = '困惑度')
  points(x_s,i2000[1,6:ncol(test)][x_s],pch=0,cex=0.7)
  lines(x,i2000[2,6:ncol(test)],type='l')
  points(x_s,i2000[2,6:ncol(test)][x_s],pch=1,cex=0.7)
  lines(x,i2000[3,6:ncol(test)],type='l')
  points(x_s,i2000[3,6:ncol(test)][x_s],pch=2,cex=0.7)
  lines(x,i2000[4,6:ncol(test)],type='l')
  points(x_s,i2000[4,6:ncol(test)][x_s],pch=3,cex=0.7)
  lines(x,i2000[5,6:ncol(test)],type='l')
  points(x_s,i2000[5,6:ncol(test)][x_s],pch=4,cex=0.7)
  legend("topright", pch=0:4,title = '主题数',
         legend=c(5,20,50,100,200))
  dev.off()
  
}

for (x in w)
{
  df = a[a['文档数']==x,]
  get_large_res(df)
}
x
