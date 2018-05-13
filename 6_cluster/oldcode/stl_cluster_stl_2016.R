res = read.csv('C:\\Elara\\Documents\\paper\\analysisi\\res_noweek.csv')
library(stlplus)
k_vector <- 2:100
source('C:/Elara/Documents/paper/cluster/ocluster.R')

#from2015

get_ocluster <- function(x_vector, k_vector, type) {
  
  path = 'C:\\Elara\\Documents\\paper\\cluster\\cluster_res\\stl_from2016\\'
  
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

#stl2015
stockdata = list()
j = 1
for (i in as.vector(unique(res['name'])[, ]))
{
  stockdata[[j]] = res[res['name'] == i &
                         res['date'] >= 20160229, ]
  
  names(stockdata)[j] = i
  j = j + 1
}


temp = list()
for (i in 1:length(stockdata)) {
  name <- names(stockdata[i])
  volume = ts(stockdata[[i]]['volume'][,],frequency = 5)
  bbb =stlplus(volume,s.window = 'periodic',fc.window = 21,fc.degree = 1)
  save(
    bbb,
    file = stringr::str_c(
      'C:\\Elara\\Documents\\paper\\cluster\\stldata\\from2016\\',
      name,
      '.RData'
    )
  )
  data_trend_all <- data.frame(volume = bbb$data$trend, date = stockdata[[i]]['date'])
  data_fc_all <- data.frame(volume = bbb$fc$fc.21, date = stockdata[[i]]['date'])
  trend_all <-
    list(
      x = data_trend_all$volume,
      time = data_trend_all$date,
      name = stringr::str_c(name, 'stl2016_trend_all')
    )

  fc_all <-
    list(
      x = data_fc_all$volume,
      time = data_fc_all$date,
      name = stringr::str_c(name, 'stl2016_fc_all')
    )

  temp <- append(temp, list(trend_all))
  temp <- append(temp, list(fc_all))
}

require(parallel)
clnum <- 3
cl <-
  makeCluster(getOption("cl.cores", clnum), outfile = 'C:\\Elara\\Documents\\paper\\cluster\\Rout.txt')
clusterExport(cl, ls())
result <- parLapply(cl, temp,  function(x)
  get_ocluster(x, k_vector))
stopCluster(cl)
save.image('C:\\Elara\\Documents\\paper\\cluster\\cluster_res\\result_stl_2016.RData')

########################################################################################################################################################

#powershell Get-Content C:\Elara\Documents\paper\cluster\Rout.txt -wait
