require(stringr)
require(data.table)
require(wordcloud2)
require(htmlwidgets)
file_path = 'C:\\Elara\\Documents\\paper\\3_analysisi\\frequency_table\\'
pic_path = 'C:\\Elara\\Documents\\paper\\3_analysisi\\frequency_table\\pic\\'
all_file = str_c(file_path,'merged_frequency_all.csv')
all_data = fread(all_file,encoding = 'UTF-8')
names(all_data) <- c('word','cnt')
# 总体
plot(all_data$cnt[1:100]/1000,type='o',ylab='词频（千次）',xlab='词编号（按词频降序排列）')
al = wordcloud2(all_data[1:100,],minRotation = 0, maxRotation = 0)
saveWidget(al,"1.html",selfcontained = F)
webshot::webshot("1.html",str_c(pic_path,"0.png"),vwidth = 1400, vheight = 900, delay =10)

stockfiles = dir(str_c(file_path,'stocks\\'))
write.csv(as.data.frame(stockfiles),'C:\\Elara\\Documents\\paper\\3_analysisi\\frequency_table\\pic\\name.csv')
st <- NULL
for (n in 1:length(stockfiles))
{
  stock_file = str_c(file_path,'stocks\\',stockfiles[n])
  stock_data = readr::read_csv(stock_file,col_names = c('word','cnt'))
  
  st <- wordcloud2(stock_data[1:100,],minRotation = 0, maxRotation = 0)
  
  saveWidget(st,"1.html",selfcontained = F)
  webshot::webshot("1.html",str_c(pic_path,n,".png"),vwidth = 1400, vheight = 900, delay =10)
}

