library(RMySQL)
con_lcl_master <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")
rds_check <- dbGetQuery(conn = con_lcl_master, statement = "SELECT version();")
funds_ids <- dbGetQuery(conn = con_lcl_master, "SELECT FACTSET_FUND_ID FROM fund_ids_us")

# Setting up parallel processing ------
library(parallel)
library(foreach)
library(doParallel)
# decide on the number of cores to use
no_cores <- detectCores() - 2

# Setting up workers and creating a MySQL connection
cl <- makeCluster(no_cores)
registerDoParallel(cl)
getDoParWorkers() # Get the number of register workers

# creating a MySQL connection on each workers
clusterEvalQ(cl, {
  library(RMySQL)
  con_lcl <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")
})

# con_lcl <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")

verbose <- TRUE
Symbols_Results_list <- foreach(k = 1:dim(funds_ids)[1],
                                .combine = list, .multicombine = TRUE,
                                .maxcombine = dim(funds_ids)[1] + 1, .export = NULL, .noexport= c('con_lcl_master'), 
                                .packages = c("caret","glmnet", "ROCR", "randomForest", "conformal", "aws.s3")) %dopar%  {
                   # for(k in 1:dim(funds_ids)[1]) {                          

                        f <- funds_ids$FACTSET_FUND_ID[k]
                        qry <- paste0("SELECT distinct(Symbol) FROM fund_us_holdings_hist_w_symbols_and_sic where FACTSET_FUND_ID = '",f , "'")
                        res <- tryCatch(expr = dbGetQuery(con_lcl, qry), error = function(cond) return(cond))
                        # Re-connect to db and fetching the data set in case the initial attempt produced an error
                        if(any(class(res) == "error")) {
                          con_lcl <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")
                          res <- dbGetQuery(con_lcl, qry)
                        }
                        res$fund <- f
                        if(verbose == TRUE) {
                          # Creating a node specific files to write debug info to
                          # fname <- paste(Sys.info()[['nodename']], Sys.getpid(), sep='-')
                          # Creating a single (local) file to write diagnostic to:
                          fname <- paste("Verbose_Symbols_per_funds", Sys.Date(), sep='-')
                          # message(paste0("Processing fund:", f ,". Memory used: ", pryr::mem_used()))
                          verbose_msg <- paste0("k = ", k,", Processing fund:", f)
                          cat(verbose_msg, file = paste0(fname, ".txt"), sep = "\n", append = TRUE)
                          # print(verbose_msg)
                        }
                        return(res)
                      }

Symbols_Results <- data.table::rbindlist(Symbols_Results_list) %>% dplyr::group_by(Symbol) %>% 
                   dplyr::summarise(Count_fund = n()) %>% dplyr::arrange(dplyr::desc(Count_fund))


TICKER_EXCH_Results_list <- foreach(k = 1:dim(funds_ids)[1],
                                    .combine = list, .multicombine = TRUE,
                                    .maxcombine = dim(funds_ids)[1] + 1, .export = NULL, .noexport= c('con_lcl_master'), 
                                    .packages = c("caret","glmnet", "ROCR", "randomForest", "conformal", "aws.s3")) %dopar%  {
                                  # for(k in 1:dim(funds_ids)[1]) {                          
                                  
                                  f <- funds_ids$FACTSET_FUND_ID[k]
                                  qry <- paste0("SELECT distinct(TICKER_EXCHANGE) FROM fund_us_holdings_hist_w_symbols_and_sic where FACTSET_FUND_ID = '",f , "'")
                                  res <- tryCatch(expr = dbGetQuery(con_lcl, qry), error = function(cond) return(cond))
                                  # Re-connect to db and fetching the data set in case the initial attempt produced an error
                                  if(any(class(res) == "error")) {
                                    con_lcl <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")
                                    res <- dbGetQuery(con_lcl, qry)
                                  }
                                  res$fund <- f
                                  if(verbose == TRUE) {
                                    # Creating a node specific files to write debug info to
                                    # fname <- paste(Sys.info()[['nodename']], Sys.getpid(), sep='-')
                                    # Creating a single (local) file to write diagnostic to:
                                    fname <- paste("Verbose_TICKER_EXCH_per_funds", Sys.Date(), sep='-')
                                    # message(paste0("Processing fund:", f ,". Memory used: ", pryr::mem_used()))
                                    verbose_msg <- paste0("k = ", k,", Processing fund:", f)
                                    cat(verbose_msg, file = paste0(fname, ".txt"), sep = "\n", append = TRUE)
                                    # print(verbose_msg)
                                  }
                                  return(res)
                                }

TICKER_EXCH_Results <- data.table::rbindlist(TICKER_EXCH_Results_list) %>% dplyr::group_by(TICKER_EXCHANGE) %>% 
                       dplyr::summarise(Count_fund = n())  %>% dplyr::arrange(dplyr::desc(Count_fund))



clusterEvalQ(cl, {
  dbDisconnect(con_lcl)
  rm(con_lcl)
})
stopCluster(cl)
rm(cl)
# Processing Results