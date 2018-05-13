

kOCluster <- function(x, k_vector) {
  for (j in k_vector) {
    if (j > length(x)) {
      stop('k>x')
      return()
    }
  }
  

  dstc <- function(x, i, j) {
    iniD <- 0
    if (i < j) {
      for (l in i:j) {
        iniD <- iniD + crossprod(x[l,] -
                                   apply(matrix(x[i:j,], j - i + 1), 2, mean))
      }
    }
    return(round(iniD, 3))
  }
  
  OCluster <- function(x, k_vector) {
    n <- nrow(x)
    mtrxD <- matrix(NA, n - 1, n - 1)
    for (a in 2:n) {
      for (b in 1:(a - 1)) {
        mtrxD[a - 1, b] <- dstc(x, b, a)
      }
    }
    final_result <- list()
    final_result[['k_vector']] <- k_vector
    final_result[['mtrxD']] <- mtrxD
    mtrxE <- matrix(NA, n - 2, n - 2)
    mtrxIndex <- matrix(NA, n - 2, n - 2)
    for (a in 3:n) {
      iniMin <- dstc(x, 1, 2 - 1) + dstc(x, 2, a)
      mtrxE[a - 2, 1] <- iniMin
      mtrxIndex[a - 2, 1] <- 2
      for (p in 3:a) {
        if (dstc(x, 1, p - 1) + dstc(x, p, a) < iniMin) {
          iniMin <- dstc(x, 1, p - 1) + dstc(x, p, a)
          mtrxE[a - 2, 1] <- iniMin
          mtrxIndex[a - 2, 1] <- p
        }
      }
    }
    for (a in 4:n) {
      for (b in 3:(a - 1)) {
        iniMin <- dstc(x, b, a)
        mtrxE[a - 2, b - 1] <- iniMin
        mtrxIndex[a - 2, b - 1] <- b
        for (p in (b + 1):a) {
          if (mtrxE[p - 3, b - 2] + dstc(x, p, a) < iniMin) {
            iniMin <- mtrxE[p - 3, b - 2] + dstc(x, p, a)
            mtrxE[a - 2, b - 1] <- iniMin
            mtrxIndex[a - 2, b - 1] <- p
          }
        }
      }
    }
    
    final_result[['mtrxE']] <- mtrxE
    final_result[['mtrxIndex']] <- mtrxIndex
    
    
    
    
    error_result <- list()
    for (k in k_vector){
      splt <- NULL
      rownum <- n
      for (p in k:2) {
        splt <- c(mtrxIndex[rownum - 2, p - 1], splt)
        rownum <- mtrxIndex[rownum - 2, p - 1] - 1
      }
      result <- NULL
      for (p in 1:n) {
        #result <- cat(result, p, " ")
        result <- stringr::str_c(result, p, " ")
        for (w in 1:length(splt)) {
          if (p == splt[w] - 1) {
            result <- stringr::str_c(result, "// ")
          }
        }
      }
      error_result[[as.character(k)]] <- result
    }
    final_result[['error_result']] <- error_result
    return(final_result)
  }
  return(OCluster(x, k_vector))
}

  

