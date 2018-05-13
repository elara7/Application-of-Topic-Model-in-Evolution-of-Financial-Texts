res = read.csv('C:\\Elara\\Documents\\paper\\analysisi\\res_noweek.csv')
library(stlplus)
k_vector <- 2:100
source('C:/Elara/Documents/paper/cluster/ocluster.R')
get_ocluster <- function(x_vector, k_vector, type) {
  
  path = 'C:\\Elara\\Documents\\paper\\cluster\\cluster_res\\raw\\'
  
  tryCatch({
    x <- x_vector[['x']]
    time <- x_vector[['time']]
    name <- x_vector[['name']]
    print(stringr::str_c('running ', name))
    x <- matrix(x, length(x), 1)
    res <- ocluster(x, k_vector)
    temp = list(
      name = name,
      res = res,
      time = time,
      raw = x
    )
    save(
      temp,
      file = stringr::str_c(
        path,
        name,
        '.RData'
      )
    )
    print(stringr::str_c('done  ', name))
    return(temp)
  }, warning = function(w) {
    print(w)
  }, error = function(e) {
    print(e)
    print(stringr::str_c('failed ', name))
    return(list(x_vector[['name']]))
  })
}

#raw 有去掉缺失
stockdata = list()
j = 1
for (i in as.vector(unique(res['name'])[, ]))
{
  stockdata[[j]] = res[res['name'] == i &
                         res['date'] >= 20150105 & !is.na(res['volume']), ]
  
  names(stockdata)[j] = i
  j = j + 1
}


temp = list()
for (i in 1:length(stockdata)) {
  name <- names(stockdata[i])
  data_all <- stockdata[[i]]
  data_2016 <- stockdata[[i]][stockdata[[i]]$date >= 20160229, ]
  volume_all <-
    list(
      x = data_all$volume,
      time = data_all$date,
      name = stringr::str_c(name, 'volume_all')
    )
  volume_2016 <-
    list(
      x = data_2016$volume,
      time = data_2016$date,
      name = stringr::str_c(name, 'volume_2016')
    )
  temp <- append(temp, list(volume_all))
  temp <- append(temp, list(volume_2016))
}

require(parallel)
clnum <- 4
cl <-
  makeCluster(getOption("cl.cores", clnum), outfile = 'C:\\Elara\\Documents\\paper\\cluster\\Rout.txt')
clusterExport(cl, ls())
result <- parLapply(cl, temp,  function(x)
  get_ocluster(x, k_vector))
stopCluster(cl)
save.image('C:\\Elara\\Documents\\paper\\cluster\\cluster_res\\raw\\result_raw.RData')

#powershell Get-Content C:\Elara\Documents\paper\cluster\Rout.txt -wait