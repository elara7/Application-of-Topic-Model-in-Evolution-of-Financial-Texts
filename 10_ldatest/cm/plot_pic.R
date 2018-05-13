setwd('D:/paper_c/Elara/Documents/paper/10_ldatest/cm')
library(stringr)

plot_pic <- function(a,m)
{
  names(a)=c('文档数','字典长度','主题数','迭代次数','C_v',1:300)
  plot(a[,'主题数'],a[,'C_v'],xlab='主题数',ylab='C_v',type='l')
  points(a[,'主题数'],a[,'C_v'],pch=16)
  title(m,cex=0.5)
}

png('res.png',res = 100,width = 700,height = 400)
par(mfrow=c(2,3))
par(mar = c(4, 4,2, 0.7))

a = read.csv('6cm_test_res.csv',header = FALSE)
plot_pic(a,'文档集A')
a = read.csv('88cm_test_res.csv',header = FALSE)
plot_pic(a,'文档集B')
a = read.csv('177cm_test_res.csv',header = FALSE)
plot_pic(a,'文档集C')
a = read.csv('681cm_test_res.csv',header = FALSE)
plot_pic(a,'文档集D')
a = read.csv('1413cm_test_res.csv',header = FALSE)
plot_pic(a,'文档集E')
a = read.csv('2073cm_test_res.csv',header = FALSE)
plot_pic(a,'文档集F')
dev.off()


get_res <- function(df)
{
  
  i500 = df[df['迭代次数']==500,]
  i1000 = df[df['迭代次数']==1000,]
  i2000 = df[df['迭代次数']==2000,]
  x = 1:100
  x_s = seq(1,100,10)
  max_y = max(df[,6:ncol(df)])
  min_y = min(df[,6:ncol(df)])
  
  file_name = str_c('C:/Elara/Documents/paper/ldatest/pic/',df[1,'文档数'],'_',500,'.png')
  png(file_name,res = 100,width = 700,height = 400)
  par(mar = c(4, 4,3, 0.7))
  plot(x,i500[1,6:ncol(i500)],type='l',ylim=c(min_y,max_y),xlim = c(1,120),xlab='遍历次数',ylab = '困惑度')
  points(x_s,i500[1,6:ncol(i500)][x_s],pch=0,cex=0.7)
  lines(x,i500[2,6:ncol(i500)],type='l')
  points(x_s,i500[2,6:ncol(i500)][x_s],pch=1,cex=0.7)
  lines(x,i500[3,6:ncol(i500)],type='l')
  points(x_s,i500[3,6:ncol(i500)][x_s],pch=2,cex=0.7)
  lines(x,i500[4,6:ncol(i500)],type='l')
  points(x_s,i500[4,6:ncol(i500)][x_s],pch=3,cex=0.7)
  lines(x,i500[5,6:ncol(i500)],type='l')
  points(x_s,i500[5,6:ncol(i500)][x_s],pch=4,cex=0.7)
  lines(x,i500[6,6:ncol(i500)],type='l')
  points(x_s,i500[6,6:ncol(i500)][x_s],pch=6,cex=0.7)
  abline(v=20,lty=1)
  legend("topright", pch=c(0:4,6),title = '主题数',
         legend=c(5,20,50,100,200,300))
  dev.off()
  
  file_name = str_c('C:/Elara/Documents/paper/ldatest/pic/',df[1,'文档数'],'_',1000,'.png')
  png(file_name,res = 100,width = 700,height = 400)
  par(mar = c(4, 4,3, 0.7))
  plot(x,i1000[1,6:ncol(i1000)],type='l',ylim=c(min_y,max_y),xlim = c(1,120),xlab='遍历次数',ylab = '困惑度')
  points(x_s,i1000[1,6:ncol(i1000)][x_s],pch=0,cex=0.7)
  lines(x,i1000[2,6:ncol(i1000)],type='l')
  points(x_s,i1000[2,6:ncol(i1000)][x_s],pch=1,cex=0.7)
  lines(x,i1000[3,6:ncol(i1000)],type='l')
  points(x_s,i1000[3,6:ncol(i1000)][x_s],pch=2,cex=0.7)
  lines(x,i1000[4,6:ncol(i1000)],type='l')
  points(x_s,i1000[4,6:ncol(i1000)][x_s],pch=3,cex=0.7)
  lines(x,i1000[5,6:ncol(i1000)],type='l')
  points(x_s,i1000[5,6:ncol(i1000)][x_s],pch=4,cex=0.7)
  lines(x,i1000[6,6:ncol(i1000)],type='l')
  points(x_s,i1000[6,6:ncol(i1000)][x_s],pch=6,cex=0.7)
  abline(v=20,lty=1)
  legend("topright", pch=c(0:4,6),title = '主题数',
         legend=c(5,20,50,100,200,300))
  dev.off()
  
  file_name = str_c('C:/Elara/Documents/paper/ldatest/pic/',df[1,'文档数'],'_',2000,'.png')
  png(file_name,res = 100,width = 700,height = 400)
  par(mar = c(4, 4,3, 0.7))
  plot(x,i2000[1,6:ncol(i2000)],type='l',ylim=c(min_y,max_y),xlim = c(1,120),xlab='遍历次数',ylab = '困惑度')
  points(x_s,i2000[1,6:ncol(i2000)][x_s],pch=0,cex=0.7)
  lines(x,i2000[2,6:ncol(i2000)],type='l')
  points(x_s,i2000[2,6:ncol(i2000)][x_s],pch=1,cex=0.7)
  lines(x,i2000[3,6:ncol(i2000)],type='l')
  points(x_s,i2000[3,6:ncol(i2000)][x_s],pch=2,cex=0.7)
  lines(x,i2000[4,6:ncol(i2000)],type='l')
  points(x_s,i2000[4,6:ncol(i2000)][x_s],pch=3,cex=0.7)
  lines(x,i2000[5,6:ncol(i2000)],type='l')
  points(x_s,i2000[5,6:ncol(i2000)][x_s],pch=4,cex=0.7)
  lines(x,i2000[6,6:ncol(i2000)],type='l')
  points(x_s,i2000[6,6:ncol(i2000)][x_s],pch=6,cex=0.7)
  abline(v=20,lty=1)
  legend("topright", pch=c(0:4,6),title = '主题数',
         legend=c(5,20,50,100,200,300))
  dev.off()
  
}




for (x in w)
{
  df = a[a['文档数']==x,]
  get_res(df)
}
x
