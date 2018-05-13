res = read.csv('C:\\Elara\\Documents\\paper\\analysisi\\res_noweek.csv')
library(stlplus)
stockdata = list()
j = 1
for (i in as.vector(unique(res['name'])[, ]))
{
  stockdata[[j]] = res[res['name'] == i &
                         res['date'] >= 20150105 & !is.na(res['volume']), ]
  
  names(stockdata)[j] = i
  j = j + 1
}




get_ocluster <- function(x_vector, k_vector) {
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
        'C:\\Elara\\Documents\\paper\\analysisi\\cluster_raw_all\\',
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
  if(name == '信威集团')
  {
    temp <- append(temp, list(volume_2016))
  }
  #temp <- append(temp, list(volume_2016))
}

#x <- list(x = matrix(c(9.3, 1.8, 1.9, 1.7, 1.5, 1.3, 1.4, 2, 1.9, 2.3, 2.1)[1:5], 5, 1),name='test1')
#y <- list(x = matrix(c(9.3, 1.8, 1.9, 1.7, 1.5, 1.3, 1.4, 2, 1.9, 2.3, 2.1), 11, 1),name='test2')
#temp=list()
#temp <- append(temp,list(x))
#temp <- append(temp,list(y))

k_vector <- 2:200


require(parallel)
clnum <- 3
cl <-
  makeCluster(getOption("cl.cores", clnum), outfile = 'C:\\Elara\\Documents\\paper\\analysisi\\Rout.txt')
clusterExport(cl, ls())
result <- parLapply(cl, temp,  function(x)
  get_ocluster(x, k_vector))
stopCluster(cl)
save.image('C:\\Elara\\Documents\\paper\\analysisi\\result_raw_all.RData')

#powershell Get-Content C:\Elara\Documents\paper\analysisi\Rout.txt -wait