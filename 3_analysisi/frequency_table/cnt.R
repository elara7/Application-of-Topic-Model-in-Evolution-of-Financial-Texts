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
al = unlist(all_data[1:100,1])

stockfiles = dir(str_c(file_path,'stocks\\'))
for (n in 1:length(stockfiles))
{
  stock_file = str_c(file_path,'stocks\\',stockfiles[n])
  stock_data = readr::read_csv(stock_file,col_names = c('word','cnt'))
  
  st <- unlist(stock_data[1:100,1])
  al <- intersect(al,st)
}

al

stopword1 = c('公司','数据','情况','时间','比例','记着','人士','领域','同比','水平','整体','原因','数量',
              '年度','因素','股份有限公司','过程','事实上','有限责任公司','万元','亿元','千元','万亿','亿万',
              '重点关注','基础上','比重','实际上')
