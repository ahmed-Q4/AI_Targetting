# https://github.com/cran/HighDimOut/blob/master/R/HighDimOut.R
Func.ABOD <- function(data, basic=FALSE, perc) {
  i=j=NULL
  if(basic==T) {
    Num_Obs <- dim(data)[1]
    res <- foreach(i = 1:(dim(data)[1]),.packages = "tcltk", .combine = c) %dopar% {
      print(paste("Processing sample i out of", dim(data)[1]))
      if(!exists("pb")) pb <- tkProgressBar("Parallel task", min=1, max=Num_Obs)
      setTkProgressBar(pb, i)
      obs <- data[i,]
      com <- t(combn(x = c(1:dim(data)[1])[-i], m = 2))
      
      cos.angles <- foreach(j = 1:(dim(com)[1]), .combine = c) %do% {
        vec.1 <- data[com[j,1],] - obs
        vec.2 <- data[com[j,2],] - obs    
        round(acos(sum(vec.1 * vec.2)/(sqrt(sum(vec.1^2))*sqrt(sum(vec.2^2)+0.01)))/(sqrt(sum(vec.1^2))*sqrt(sum(vec.2^2)+0.01)), digits = 2)
      }
      return(var(x = cos.angles))
    }
    return(res) 
  } else {
    nu <- round(dim(data)[1]*perc, digits = 0)
    Num_Obs <- dim(data)[1]
    res <- foreach(i = 1:(dim(data)[1]), .packages = "tcltk",  .combine = c) %dopar% {
      # http://blog.revolutionanalytics.com/2015/03/creating-progress-bar-with-foreach-parallel-processing.html
      print(paste("Processing sample i out of", dim(data)[1]))
      if(!exists("pb")) pb <- tkProgressBar("Parallel task", min=1, max=Num_Obs)
      setTkProgressBar(pb, i)
      obs <- data[i,]
      index.used <- sample(x = c(1:dim(data)[1])[-i], size = nu, replace = F)
      com <- t(combn(x = index.used, m = 2))
      
      cos.angles <- foreach(j = 1:(dim(com)[1]), .combine = c) %do% {
        vec.1 <- data[com[j,1],] - obs
        vec.2 <- data[com[j,2],] - obs    
        round(acos(sum(vec.1 * vec.2)/(sqrt(sum(vec.1^2))*sqrt(sum(vec.2^2)+0.01)))/(sqrt(sum(vec.1^2))*sqrt(sum(vec.2^2)+0.01)), digits = 2)
      }
      return(var(x = cos.angles))
    }
    return(res)    
  }
}