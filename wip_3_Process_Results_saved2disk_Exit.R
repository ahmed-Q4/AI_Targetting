library(parallel)
library(foreach)
library(doParallel)
# decide on the number of cores to use
no_cores <- min(detectCores() - 1, 30)
#no_cores <- 1

# Setting up workers and creating a MySQL connection
cl <- makeCluster(no_cores)
registerDoParallel(cl)
getDoParWorkers() # Get the number of register workers

fundsList_analyzed <- dir(path = "./Results_List/")
ff <- fundsList_analyzed[1]
res_tmp <- readRDS(paste0("./Results_List/", ff))
ListVar <- names(res_tmp)

getResults <- function(metric) {
  res <- foreach(k = 1:length(fundsList_analyzed),
                 .combine = list, .multicombine = TRUE,
                 .maxcombine = length(fundsList_analyzed) + 1, .export = c("fundsList_analyzed"), .noexport= NULL, 
                 .packages = NULL) %dopar%  {
                   
                   ff <- fundsList_analyzed[k]
                   res_tmp <- readRDS(paste0("./Results_List/", ff))
                   
                   return(res_tmp[[metric]])
                 }
  return(res)
}

Results_stats_ExitPosition <- getResults("Results_stat")
Results_stats_ExitPosition <- dplyr::bind_rows(Results_stats_ExitPosition)

Results_stats_numeric_ExitPosition <- dplyr::select(Results_stats_ExitPosition, -Prob_pred_Buy_given_Buy_Lasso, -Prob_pred_Buy_given_Sell_Lasso,
                                                     -Prob_pred_Sell_given_Buy_Lasso, -Prob_pred_Sell_given_Sell_Lasso) %>%
  dplyr::mutate(Prob_No_Exit_given_pred_No_Exit = sapply(strsplit(Prob_Buy_given_pred_Buy_Lasso,  split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                Prob_No_Exit_given_pred_Exit    = sapply(strsplit(Prob_Buy_given_pred_Sell_Lasso, split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                Prob_Exit_given_pred_No_Exit    = sapply(strsplit(Prob_Sell_given_pred_Buy_Lasso, split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                Prob_Exit_given_pred_Exit       = sapply(strsplit(Prob_Sell_given_pred_Sell_Lasso,split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                Pred_Accuracy                     = sapply(strsplit(Pred_Accuracy_GLMnet_Lasso,     split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100)) %>%
  dplyr::select(-dplyr::contains("_Lasso"))






Results_warning_ExitPosition <- lapply(fundsList_analyzed, FUN = function(x){
  res_tmp <- readRDS(paste0("./Results_List/", x))
  res <- res_tmp$RegFit_warning
  return(res)
}) 
Results_warning_ExitPosition <- Results_warning_ExitPosition %>% unlist()
length(which(Results_warning_ExitPosition == TRUE))

Results_Error_ExitPosition <- lapply(fundsList_analyzed, FUN = function(x){
  res_tmp <- readRDS(paste0("./Results_List/", x))
  res <- res_tmp$RegFit_error
  return(res)
}) 
Results_Error_ExitPosition <- Results_Error_ExitPosition %>% unlist()
length(which(Results_Error_ExitPosition == TRUE))


Results_InsufficientData_ExitPosition <- lapply(fundsList_analyzed, FUN = function(x){
  res_tmp <- readRDS(paste0("./Results_List/", x))
  res <- res_tmp$Insufficient_data
  return(res)
}) 
Results_InsufficientData_ExitPosition <- Results_InsufficientData_ExitPosition %>% unlist()
length(which(Results_InsufficientData_ExitPosition == TRUE))


Results_InfoMsg_ExitPosition <- lapply(fundsList_analyzed, FUN = function(x){
  res_tmp <- readRDS(paste0("./Results_List/", x))
  res <- res_tmp$Info.Msg
  return(res)
}) 
Results_InfoMsg_ExitPosition <- Results_InfoMsg_ExitPosition %>% unlist() %>% as.data.frame()
NROW(Results_InfoMsg_ExitPosition)

stopCluster(cl)
rm(cl)


