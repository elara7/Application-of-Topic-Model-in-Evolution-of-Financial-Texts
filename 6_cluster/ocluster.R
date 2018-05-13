ocluster = function(datasam, classnum) {
  for (j in 1:length(classnum)) {
    if (classnum[j] > length(datasam)) {
      print(c('k>x',length(datasam),classnum[j]))
      classnum[j] <- length(datasam)
    }
  }
  #datasam为样本数据阵，每一行为一个样本，一列为一个特征；
  #classnum为要分的类数向量
  #ra_dis:距离矩阵 leastlost:最小损失矩阵 classid:分类标识矩阵
  

  datasam <- as.matrix(datasam)
  sam_n = dim(datasam)[1]
  
  #计算i-j个样本组成的类的半径 
  radi = function(a) {
    i <- min(a)
    j <- max(a)
    #提取i-j个样本
    temp = as.matrix(datasam[i:j,])
    mu = colMeans(matrix(temp, j - i + 1))
    vec = apply(matrix(temp, j - i + 1), 1, function(x) {
      x - mu
    })
    round(sum(apply(matrix(vec, j - i + 1), 2, crossprod)), 3) 
  }
  rd_temp <- as.matrix(expand.grid(1:sam_n , 1:sam_n))
  colnames(rd_temp) <- NULL
  ra_dis <-
    matrix(apply(rd_temp , 1 , radi) , nrow = sam_n , ncol = sam_n)
 
  leastlost = matrix(NA, sam_n - 1, sam_n - 1)
  rownames(leastlost) = 2:sam_n
  colnames(leastlost) = 2:sam_n
  diag(leastlost) = 0

  

  classid = matrix(NA, sam_n - 1, sam_n - 1)
  rownames(classid) = 2:sam_n
  colnames(classid) = 2:sam_n
  diag(classid) = 2:sam_n
  

  leastlost[as.character(3:sam_n), "2"] = sapply(3:sam_n,
                                                 function(xn) {
                                                   min(ra_dis[1, 1:(xn - 1)] + ra_dis[2:xn, xn])
                                                 })
  classid[as.character(3:sam_n), "2"] = sapply(3:sam_n, function(xn) {
    which((ra_dis[1, 1:(xn - 1)] + ra_dis[2:xn, xn]) == (min(ra_dis[1,
                                                                    1:(xn - 1)] + ra_dis[2:xn, xn])))[1] + 1
  })

  
  for (j in as.character(3:(sam_n - 1))) {
    leastlost[as.character((as.integer(j) + 1):sam_n), j] = sapply((as.integer(j) +
                                                                      1):sam_n, function(xn) {
                                                                        min(leastlost[as.character(j:xn - 1), as.character(as.integer(j) -
                                                                                                                             1)] + ra_dis[j:xn, xn])
                                                                      })
    
    classid[as.character((as.integer(j) + 1):sam_n), j] = sapply((as.integer(j) +
                                                                    1):sam_n, function(xn) {
                                                                      a = which((leastlost[as.character(j:xn - 1), as.character(as.integer(j) -
                                                                                                                                  1)] + ra_dis[j:xn, xn]) == min(leastlost[as.character(j:xn -
                                                                                                                                                                                          1), as.character(as.integer(j) - 1)] + ra_dis[j:xn,
                                                                                                                                                                                                                                        xn]))[1] + as.integer(j) - 1
                                                                    })
  }
  
  diag(classid) = 2:sam_n
  
  final_result <- list()
  final_result[['distance_matrix']] <- ra_dis[2:sam_n, 1:(sam_n - 1)]
  final_result[['leastlost_matrix']] <- leastlost[2:(sam_n - 1), 1:(sam_n - 2)]
  final_result[['classid_matrix']] <- classid[2:(sam_n - 1), 1:(sam_n - 2)]
  
  # 查询不同类数下的分法
  get_result <- function(k,classid){
    breaks = rep(0, 1, k)
    breaks[1] = 1
    breaks[k] = classid[as.character(sam_n), as.character(k)]
    flag = k - 1
    while (flag >= 2) {
      breaks[flag] = classid[as.character(breaks[flag + 1] -
                                            1), as.character(flag)]
      flag = flag - 1
    }
    result1 = NULL
    for (p in 1:sam_n) {
      result1 <- stringr::str_c(result1, p, " ")
      for (w in 1:length(breaks)) {
        if (p == breaks[w] - 1) {
          result1 <- stringr::str_c(result1, "||")
        }
      }
      if (p == sam_n)
        result1 = stringr::str_c(result1, " ")
      
    }
    return(list(k = k,result = result1))
  }
  
  res <- lapply(classnum,function(x) get_result(x,classid))

  final_result[['result']] <- res
  return(final_result)
}


# 绘制碎石图
# sam_n = dim(datasam)[1]
# leastlost = final_result[['leastlost_matrix']]
# plot(
#   leastlost[nrow(leastlost), ] ,
#   type = "b" ,
#   main = "最小损失函数随分类数变化的趋势图",
#   xaxt = "n" ,
#   xlab = "分类数" ,
#   ylab = "最小损失函数" ,
#   col = "blue"
# )
# axis(
#   1,
#   at = 1:(sam_n - 1) ,
#   labels = 1:(sam_n - 1) ,
#   las = 0
# )


