res = read.csv('C:\\Elara\\Documents\\paper\\analysisi\\res_noweek.csv')
library(stlplus)
stockdata=list()
j=1
for (i in as.vector(unique(res['name'])[,]))
{
  stockdata[[j]] = res[res['name'] == i & res['date'] >= 20150105,]

    names(stockdata)[j] = i
  j = j+1
}

stldata <- list()
for (i in 1:length(stockdata))
{
  #num = ts(stockdata[[i]]['num'][,],frequency = 5)
  volume = ts(stockdata[[i]]['volume'][,],frequency = 5)
  #aaa =stlplus(num,s.window = 'periodic',fc.window = 20,fc.degree = 1)
  #plot(aaa)
  bbb =stlplus(volume,s.window = 'periodic',fc.window = 21,fc.degree = 1)
  #plot(bbb)
  temp = cbind(stockdata[[i]]['name'],stockdata[[i]]['date'],bbb$data,bbb$fc)
  stldata[[names(stockdata)[i]]] <- temp
  write.csv(temp,stringr::str_c('C:\\Elara\\Documents\\paper\\analysisi\\stlfrom20150105\\',names(stockdata)[i],'.csv'))
  
}


#names(stockdata)[50]
#中国联通 同方股份 信威集团 江苏银行 上海银行 中国银河 中国重工


get_ocluster <- function(x_vector,k_vector){
  tryCatch({
    x <- x_vector[['x']]
    name <- x_vector[['name']]
    print(stringr::str_c('running ',name))
    x <- matrix(x, length(x), 1)
    res <- ocluster(x,k_vector)
    temp = list(name = name,res = res)
    save(temp,file = stringr::str_c('C:\\Elara\\Documents\\paper\\analysisi\\cluster\\',name,'.RData'))
    print(stringr::str_c('done  ',name))
    return(temp)
  }, warning = function(w) {
    print(w)
  }, error = function(e) {
    print(e)
    print(stringr::str_c('failed ',name))
    return(list(x_vector[['name']]))
  })
}

temp=list()
for (i in 1:length(stldata)){
  name <- names(stldata[i])
  data_all <- stldata[[i]]
  data_2016 <- stldata[[i]][stldata[[i]]$date>=20160229,]
  trend_all <- list(x = data_all$trend, name=stringr::str_c(name,'trend_all'))
  fc21_all <- list(x = data_all$fc.21, name=stringr::str_c(name,'fc21_all'))
  trend_2016 <- list(x = data_2016$trend, name=stringr::str_c(name,'trend_2016'))
  fc21_2016 <- list(x = data_2016$fc.21, name=stringr::str_c(name,'fc21_2016'))
  temp <- append(temp,list(trend_all))
  temp <- append(temp,list(fc21_all))
  #temp <- append(temp,list(trend_2016))
  #temp <- append(temp,list(fc21_2016))
}

#x <- list(x = matrix(c(9.3, 1.8, 1.9, 1.7, 1.5, 1.3, 1.4, 2, 1.9, 2.3, 2.1)[1:5], 5, 1),name='test1')
#y <- list(x = matrix(c(9.3, 1.8, 1.9, 1.7, 1.5, 1.3, 1.4, 2, 1.9, 2.3, 2.1), 11, 1),name='test2')
#temp=list()
#temp <- append(temp,list(x))
#temp <- append(temp,list(y))

k_vector <- 2:300


require(parallel)
clnum <- 4
cl <- makeCluster(getOption("cl.cores", clnum),outfile = 'C:\\Elara\\Documents\\paper\\analysisi\\Rout.txt')
clusterExport(cl, ls())
result <- parLapply(cl, temp,  function(x)
  get_ocluster(x, k_vector))
stopCluster(cl)
save.image('C:\\Elara\\Documents\\paper\\analysisi\\result.RData')

#powershell Get-Content C:\Elara\Documents\paper\analysisi\Rout.txt -wait