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

Results_stats_EntryPosition <- getResults("Results_stat")
Results_stats_EntryPosition <- dplyr::bind_rows(Results_stats_EntryPosition)

Results_stats_numeric_EntryPosition <- dplyr::select(Results_stats_EntryPosition, -Prob_pred_Buy_given_Buy_Lasso, -Prob_pred_Buy_given_Sell_Lasso,
                                                     -Prob_pred_Sell_given_Buy_Lasso, -Prob_pred_Sell_given_Sell_Lasso) %>%
                                       dplyr::mutate(Prob_No_Entry_given_pred_No_Entry = sapply(strsplit(Prob_Buy_given_pred_Buy_Lasso,  split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                      Prob_No_Entry_given_pred_Entry    = sapply(strsplit(Prob_Buy_given_pred_Sell_Lasso, split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                      Prob_Entry_given_pred_No_Entry    = sapply(strsplit(Prob_Sell_given_pred_Buy_Lasso, split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                      Prob_Entry_given_pred_Entry       = sapply(strsplit(Prob_Sell_given_pred_Sell_Lasso,split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                      Pred_Accuracy                     = sapply(strsplit(Pred_Accuracy_GLMnet_Lasso,     split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100)) %>%
                                       dplyr::select(-dplyr::contains("_Lasso"))






Results_warning_EntryPosition <- lapply(fundsList_analyzed, FUN = function(x){
  res_tmp <- readRDS(paste0("./Results_List/", x))
  res <- res_tmp$RegFit_warning
  return(res)
}) 
Results_warning_EntryPosition <- Results_warning_EntryPosition %>% unlist()
length(which(Results_warning_EntryPosition == TRUE))

Results_Error_EntryPosition <- lapply(fundsList_analyzed, FUN = function(x){
  res_tmp <- readRDS(paste0("./Results_List/", x))
  res <- res_tmp$RegFit_error
  return(res)
}) 
Results_Error_EntryPosition <- Results_Error_EntryPosition %>% unlist()
length(which(Results_Error_EntryPosition == TRUE))


Results_InsufficientData_EntryPosition <- lapply(fundsList_analyzed, FUN = function(x){
  res_tmp <- readRDS(paste0("./Results_List/", x))
  res <- res_tmp$Insufficient_data
  return(res)
}) 
Results_InsufficientData_EntryPosition <- Results_InsufficientData_EntryPosition %>% unlist()
length(which(Results_InsufficientData_EntryPosition == TRUE))


Results_InfoMsg_EntryPosition <- lapply(fundsList_analyzed, FUN = function(x){
  res_tmp <- readRDS(paste0("./Results_List/", x))
  res <- res_tmp$Info.Msg
  return(res)
}) 
Results_InfoMsg_EntryPosition <- Results_InfoMsg_EntryPosition %>% unlist() %>% as.data.frame()
length(which(Results_InsufficientData_EntryPosition == TRUE))


