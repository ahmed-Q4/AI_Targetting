mem_used <- function(){
  # Based on pryr::mem_used but return a text string
  x <- pryr::mem_used()
  power <- min(floor(log(abs(x), 1000)), 4)
  if (power < 1) {
    unit <- "B"
  }
  else {
    unit <- c("kB", "MB", "GB", "TB")[[power]]
    x <- x/(1000^power)
  }
  formatted <- format(signif(x, digits = digits), big.mark = ",", 
                      scientific = FALSE)
  res <- paste(formatted, unit)
  return(res)
}

dummy_fn2 <- function(x) {browser()}

library(readxl)
library(readr)
PotentialFunds <- read_excel("~/R_workspaces/AI_Targetting/Results_stats_Entry_Exit_500.xlsx", sheet = "PotentialFunds >100M, noETF ", 
                             col_types = c("text", "text", "text","blank", "text", "text", "date","text", "numeric", "date", "blank",
                                           "blank", "blank", "blank"))

library(data.table)
library(bit64)

Prices <- fread("~/R_workspaces/AI_Targetting/Data from Rob/own_prices.txt", sep = "|") %>% dplyr::mutate(PRICE_DATE = as.Date(PRICE_DATE)) 
# Example
# SampleFundData_4Analysis <- read_csv("~/R_workspaces/AI_Targetting/SampleFundData_4Analysis.csv", col_types = cols(row_names = col_skip()))
# tt <- SampleFundData_4Analysis %>% dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE")) %>%
#       dplyr::mutate(MrktVal = HOLDING * PRICE) %>%
#       dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PortVal_repDate = sum(MrktVal, na.rm = TRUE)) %>%
#       dplyr::group_by(REPORT_DATE, SECTOR_CODE) %>% dplyr::mutate(SectorExpousure = sum(MrktVal, na.rm = TRUE)/ PortVal_repDate) %>%
#       dplyr::summarise(SectorExpousure = mean(SectorExpousure,na.rm = TRUE))



# Creating MySQL connection locally ----
library(RMySQL)
# con <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel2")
con_lcl_master <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")
con_RDS_master <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")
# dbDisconnect(con)

if (Sys.getenv("TZ") == "") Sys.setenv(TZ="US/Eastern")

funds_ids <- PotentialFunds

library(parallel)
library(foreach)
library(doParallel)
# decide on the number of cores to use
no_cores <- detectCores() - 1

# Setting up workers and creating a MySQL connection
cl <- makeCluster(no_cores)
registerDoParallel(cl)
getDoParWorkers() # Get the number of register workers

# creating a MySQL connection on each workers
clusterEvalQ(cl, {
  library(RMySQL)
  con_lcl <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")
  con_RDS <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")
})

dbDisconnect(con_lcl_master)
dbDisconnect(con_RDS_master)

verbose <- TRUE
debug_flag <- FALSE

Save2S3 <- FALSE

# TO DO: Normalize and scale the features before fitting
if(debug_flag) {
  con_lcl <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")
  con_RDS <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")
  con_rds_check <- dbGetQuery(conn = con_RDS, statement = "SELECT version();")
  con_lcl_check <- dbGetQuery(conn = con_lcl, statement = "SELECT version();")
}

if(Save2S3 == TRUE) {
  S3_path <-  switch(as.character(i),
                     "1" = "/_ModelResults/1_Entry/",
                     "2" = "/_ModelResults/2_Change/",
                     "3" = "/_ModelResults/3_Exit/")
}
# browser()
t_start <- Sys.time()
Results_finals <- foreach(j = 1:dim(funds_ids)[1], #length(debug_seq), #dim(funds_ids)[1],
                          .combine = list, .multicombine = TRUE,
                          .maxcombine = dim(funds_ids)[1] + 1, .export = NULL, .noexport= c('con_lcl_master','con_RDS_master'),
                          .packages = c("caret","glmnet", "ROCR", "randomForest", "conformal", "aws.s3")) %dopar%  {
                            
                            #                                   results_debug <- list()
# for(j in 1:4){ # length(debug_seq)) { # 
#   browser()
                            #  browser()
                            #j = debug_seq[k]
                            # Setting the seed for reproducability 
                            set.seed(j)
                            #Creating a list to save results
                            
                            # Processing i_th fund
                            f <- funds_ids$FactSetEntityId[j]
                            # res2ret$Fund_ID <- f <- fund2debug$FACTSET_FUND_ID[j]
                            
                            if(verbose == TRUE) {
                              # Creating a node specific files to write debug info to
                              fname <- paste(Sys.info()[['nodename']], Sys.getpid(), sep='-')
                              # Creating a single (local) file to write diagnostic to:
                              fname <- paste("Verbose_Log", Sys.Date(), "FundAnalysis", sep='-')
                              # message(paste0("Processing fund:", f ,". Memory used: ", pryr::mem_used()))
                              verbose_msg <- paste0("Worker: ", paste(Sys.info()[['nodename']], Sys.getpid(), sep='-'),
                                                    ", j = ", j,", Processing fund:", f ,", Memory used: ", pryr::mem_used())
                              cat(verbose_msg, file = paste0(fname, ".txt"), sep = "\n", append = TRUE)
                              # print(verbose_msg)
                            }
                            
                            qry <- paste0("SELECT * FROM fund_us_holdings_hist_w_symbols_and_sic where FACTSET_FUND_ID = '",f , "'")
                            
                            # Getting Data from DB
                            data.set_000 <- tryCatch(expr = dbGetQuery(con_lcl, qry), error = function(cond) return(cond))
                            # Re-connect to db and fetching the data set in case the initial attempt produced an error
                            if(any(class(data.set_000) == "error")) {
                              con_lcl <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")
                              data.set_000 <- dbGetQuery(con_lcl, qry)
                            }
                            
                            res_sector <- data.set_000 %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>%
                              dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE")) %>%
                              dplyr::mutate(MrktVal = HOLDING * PRICE) %>%
                              dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PortVal_repDate = sum(MrktVal, na.rm = TRUE)) %>%
                              dplyr::group_by(REPORT_DATE, SECTOR_CODE) %>% dplyr::mutate(SectorExpousure = sum(MrktVal, na.rm = TRUE)/ PortVal_repDate) %>%
                              dplyr::summarise(SectorExpousure = mean(SectorExpousure,na.rm = TRUE))
                            
                            
                            res_cap <-    data.set_000 %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>% 
                                          dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE")) %>%
                                          dplyr::mutate(MrktVal = HOLDING * PRICE) %>%
                                          dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PortVal_repDate = sum(MrktVal, na.rm = TRUE)) %>%
                                          dplyr::group_by(REPORT_DATE, CAP_GROUP) %>% dplyr::mutate(CAP_Expousure = sum(MrktVal, na.rm = TRUE)/ PortVal_repDate) %>%
                                          dplyr::summarise(CAP_Expousure = mean(CAP_Expousure,na.rm = TRUE))
                            
                            
                            res_SIC <- data.set_000 %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>%
                                       dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE")) %>%
                                       dplyr::mutate(MrktVal = HOLDING * PRICE) %>%
                                       dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PortVal_repDate = sum(MrktVal, na.rm = TRUE)) %>%
                                       dplyr::group_by(REPORT_DATE, PRIMARY_SIC_CODE) %>% dplyr::mutate(SIC_Expousure = sum(MrktVal, na.rm = TRUE)/ PortVal_repDate) %>%
                                       dplyr::summarise(SIC_Expousure = mean(SIC_Expousure,na.rm = TRUE))
                              
                              
                              res_Ind <- data.set_000 %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>% 
                                         dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE")) %>%
                                         dplyr::mutate(MrktVal = HOLDING * PRICE) %>%
                                         dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PortVal_repDate = sum(MrktVal, na.rm = TRUE)) %>%
                                         dplyr::group_by(REPORT_DATE, INDUSTRY_CODE) %>% dplyr::mutate(Industry_Expousure = sum(MrktVal, na.rm = TRUE)/ PortVal_repDate) %>%
                                         dplyr::summarise(Industry_Expousure = mean(Industry_Expousure,na.rm = TRUE))
                            
                            res_sector$fund <- res_cap$fund <- res_SIC$fund <- res_Ind$fund <- f
                            
                            res <- list(Cap_Result = res_cap, Sector_Result = res_sector, 
                                        SIC_Result = res_SIC, Industry_Result = res_Ind)
                            return(res)
                          }
#browser()
if(debug_flag) Results_finals <- results_debug

t_end <- Sys.time()

clusterEvalQ(cl, {
  dbDisconnect(con_lcl)
  dbDisconnect(con_RDS)
  rm(con_lcl, con_RDS)
})
stopCluster(cl)
rm(cl)

res_CAP      <- lapply(Results_finals, function(x) x$Cap_Result)      %>% dplyr::bind_rows()
res_Sector   <- lapply(Results_finals, function(x) x$Sector_Result)   %>% dplyr::bind_rows()
res_SIC      <- lapply(Results_finals, function(x) x$SIC_Result)      %>% dplyr::bind_rows()
res_Industry <- lapply(Results_finals, function(x) x$Industry_Result) %>% dplyr::bind_rows()


hist(res_CAP$CAP_Expousure)
hist(res_Sector$SectorExpousure)
hist(res_SIC$SIC_Expousure)
hist(res_Industry$Industry_Expousure)


res_CAP_concentration <- res_CAP %>% dplyr::group_by(fund)  %>% dplyr::filter(REPORT_DATE == max(REPORT_DATE)) %>%
                         dplyr::filter(CAP_Expousure > 0.8) %>% dplyr::ungroup()

res_Sector_concentration <- res_Sector %>% dplyr::group_by(fund)  %>% dplyr::filter(REPORT_DATE == max(REPORT_DATE)) %>%
                         dplyr::filter(SectorExpousure > 0.8) %>% dplyr::ungroup()

res_SIC_concentration <- res_SIC %>% dplyr::group_by(fund)  %>% dplyr::filter(REPORT_DATE == max(REPORT_DATE)) %>%
                         dplyr::filter(SIC_Expousure > 0.8) %>% dplyr::ungroup()

res_Industry_concentration <- res_Industry %>% dplyr::group_by(fund)  %>% dplyr::filter(REPORT_DATE == max(REPORT_DATE)) %>%
                         dplyr::filter(Industry_Expousure > 0.8) %>% dplyr::ungroup()