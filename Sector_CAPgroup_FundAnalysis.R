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

matchPrices <- function(x, Prices) {
  j = 0
  y <- x %>% dplyr::left_join(Prices[, c(1:3, 4+j)], by = c("FS_PERM_SEC_ID", "REPORT_DATE" = names(Prices)[4 + j]))
  while(all(is.na(y$PRICE)) && j < 5) {
    j = j + 1
    y <- x %>% dplyr::left_join(Prices[, c(1:3, 4+j)], by = c("FS_PERM_SEC_ID", "REPORT_DATE" = names(Prices)[4 + j]))
  }
  y[[names(Prices)[4 + j]]] <- TRUE
  return(y)
}

matchPrices2 <- function(x, Prices) {
  j = 0
  res_0 <- x %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>%
    #dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PriceDate = price_dates[which.min(abs(price_dates - REPORT_DATE))]) %>%
    dplyr::left_join(Prices[, c(1:3, 4+j)], by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE"))
  
  res_tmp_good    <- res_0  %>% dplyr::filter(!is.na(PRICE))
  res_tmp_missing <- res_0  %>% dplyr::filter( is.na(PRICE))
  
  while(NROW(res_tmp_missing) > 0 && j < 6) {
    j = j + 1
    res_tmp_missing <- res_tmp_missing[, 1:14]
    
    res_tmp <- res_tmp_missing %>% dplyr::left_join(Prices[, c(1:3, 4+j)], by = c("FS_PERM_SEC_ID", "REPORT_DATE" = names(Prices)[4 + j]))
    res_tmp[[names(Prices)[4 + j]]] <- TRUE
    
    res_tmp_good    <- dplyr::bind_rows(res_tmp_good, 
                                        res_tmp  %>% dplyr::filter(!is.na(PRICE))
    )
    
    res_tmp_missing <- res_tmp %>% dplyr::filter(is.na(PRICE))
  }
  res <- res_tmp_good
  return(res)
}

cleanMem <- function(n=10) { for (i in 1:n) gc() }

library(readxl)
library(readr)
PotentialFunds <- read_excel("~/R_workspaces/AI_Targetting/Results_stats_Entry_Exit_500.xlsx", sheet = "PotentialFunds >100M, noETF ", 
                             col_types = c("text", "text", "text","blank", "text", "text", "date","text", "numeric", "date", "blank",
                                           "blank", "blank", "blank"))


GoodFund <- read_excel("~/R_workspaces/AI_Targetting/Results_stats_Entry_Exit_500.xlsx", sheet = "UniqueGoodFundsName", 
                       col_types = c("blank", "blank", "blank", "text", "text", "text", "text", "text"))

library(data.table)
library(bit64)

Prices <- fread("~/R_workspaces/AI_Targetting/Data from Rob/own_prices.txt", sep = "|") %>% dplyr::mutate(PRICE_DATE = as.Date(PRICE_DATE)) 
Prices[['PRICE_DATE-1']] <- Prices$PRICE_DATE - 1
Prices[['PRICE_DATE-2']] <- Prices$PRICE_DATE - 2
Prices[['PRICE_DATE-3']] <- Prices$PRICE_DATE - 3
Prices[['PRICE_DATE+1']] <- Prices$PRICE_DATE + 1
Prices[['PRICE_DATE+2']] <- Prices$PRICE_DATE + 2
Prices[['PRICE_DATE+3']] <- Prices$PRICE_DATE + 3

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
no_cores <- min(detectCores() - 1, 30)

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
debug_fund <- c("04BR3P-E" ,"04D5X2-E", "04CB0M-E", "04CRQY-E")

writeModelResults2Disk <- TRUE # FALSE
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
# Results_finals <- foreach(j = 1:dim(funds_ids)[1], #length(debug_seq), #dim(funds_ids)[1],
#                           .combine = list, .multicombine = TRUE,
#                           .maxcombine = dim(funds_ids)[1] + 1, .export = NULL, .noexport= c('con_lcl','con_RDS'), 
#                           .packages = c("caret","glmnet", "ROCR", "randomForest", "conformal", "aws.s3")) %dopar%  {

results_debug <- list()
for(j in 1:length(debug_fund)) {
  #j = debug_seq[k]
  # Setting the seed for reproducability 
  set.seed(j)
  #Creating a list to save results
  
  # Processing i_th fund
  f <- debug_fund[j]
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
  
  data.set <-  matchPrices2(data.set_000, Prices = Prices)
  
  if(NROW(data.set) == 0) {
    data.set <- data.set_000 %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>%
      dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE"))
    
    cat(f, file = "NoPricesMatch4Funds.txt", sep = "\n", append = TRUE)
    
  }
  
  res_sector <-  data.set %>%
    #data.set_000 %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>%
    #               dplyr::group_by(REPORT_DATE) %>% 
    #               dplyr::do(res = matchPrices(., Prices = Prices)) %$%
    #               dplyr::bind_rows(res) %>% 
    
    #dplyr::mutate(PRICE_DATE = Prices$PRICE_DATE[which.min(abs(Prices$PRICE_DATE - REPORT_DATE))]) %>%
    #dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "PRICE_DATE")) %>%
    dplyr::mutate(MrktVal = HOLDING * PRICE) %>%
    dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PortVal_repDate = sum(MrktVal, na.rm = TRUE)) %>%
    dplyr::group_by(REPORT_DATE, SECTOR_CODE) %>% dplyr::mutate(SectorExpousure = sum(MrktVal, na.rm = TRUE)/ PortVal_repDate) %>%
    dplyr::summarise(SectorExpousure = mean(SectorExpousure,na.rm = TRUE))
  
  res_cap <- data.set %>%
    #data.set_000 %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>% 
    #dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PriceDate = price_dates[which.min(abs(price_dates - REPORT_DATE))]) %>%
    #dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE")) %>%
    dplyr::mutate(MrktVal = HOLDING * PRICE) %>%
    dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PortVal_repDate = sum(MrktVal, na.rm = TRUE)) %>%
    dplyr::group_by(REPORT_DATE, CAP_GROUP) %>% dplyr::mutate(CAP_Expousure = sum(MrktVal, na.rm = TRUE)/ PortVal_repDate) %>%
    dplyr::summarise(CAP_Exposure = mean(CAP_Expousure,na.rm = TRUE))
  
  
  res_SIC <- data.set %>%
    #data.set_000 %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>%
    #dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PriceDate = price_dates[which.min(abs(price_dates - REPORT_DATE))]) %>%
    #dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE")) %>%
    dplyr::mutate(MrktVal = HOLDING * PRICE) %>%
    dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PortVal_repDate = sum(MrktVal, na.rm = TRUE)) %>%
    dplyr::group_by(REPORT_DATE, PRIMARY_SIC_CODE) %>% dplyr::mutate(SIC_Expousure = sum(MrktVal, na.rm = TRUE)/ PortVal_repDate) %>%
    dplyr::summarise(SIC_Exposure = mean(SIC_Expousure,na.rm = TRUE))
  
  
  res_Ind <- data.set %>%
    #data.set_000 %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>% 
    #dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PriceDate = price_dates[which.min(abs(price_dates - REPORT_DATE))]) %>%
    #dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE")) %>%
    dplyr::mutate(MrktVal = HOLDING * PRICE) %>%
    dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PortVal_repDate = sum(MrktVal, na.rm = TRUE)) %>%
    dplyr::group_by(REPORT_DATE, INDUSTRY_CODE) %>% dplyr::mutate(Industry_Expousure = sum(MrktVal, na.rm = TRUE)/ PortVal_repDate) %>%
    dplyr::summarise(Industry_Exposure = mean(Industry_Expousure,na.rm = TRUE))
  
  res_sector$fund <- res_cap$fund <- res_SIC$fund <- res_Ind$fund  <- Port_Size$fund <- f
  
  # Dividend yield concentration analysis - bases on Q_Ends
  #
  # Aligning Report Date with Quarter Ends.
  data.set_Q_Ends <- data.set %>% 
    dplyr::mutate(Q_Ends = as.Date(as.character(timeDate::timeLastDayInQuarter(REPORT_DATE, format = "%Y-%m-%d", zone = "", FinCenter = ""))),
                  HOLDING = as.numeric(HOLDING)) %>%
    dplyr::group_by(FACTSET_FUND_ID, TICKER_EXCHANGE, Q_Ends) %>% 
    dplyr::summarise(HOLDING_tot = sum(HOLDING, na.rm = TRUE),
                     Symbol = dplyr::first(Symbol),
                     CAP_GROUP = as.factor(dplyr::first(CAP_GROUP)),
                     SECTOR_CODE = as.factor(dplyr::first(SECTOR_CODE)),
                     LastPrice = dplyr::last(PRICE)
    ) %>% dplyr::ungroup()
  
  # Getting unique Symbol and Date List
  Symbol_list <- unique(data.set_Q_Ends$Symbol)
  Date_list   <- unique(data.set_Q_Ends$Q_Ends)
  
  # Getting Input data from RDS by querrying the 18 group tables: _gTable_01, ..., _gTable_18
  rds_check <- dbGetQuery(conn = con_RDS, statement = "SELECT version();")
  if(any(class(rds_check) == "error")) {
    con_RDS <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")
  }
  # Getting div yield from FunVar_1_FF_DIV_YLD from RDS
  qry_rds <- paste0("SELECT * FROM q4_intel.FunVar_1_FF_DIV_YLD where Symbol IN ('",
                    paste(Symbol_list, collapse = "' , '"), "')",
                    " AND Date IN ('", paste(Date_list, collapse = "' , '"), "');")
  X_Variables <- dbGetQuery(conn = con_RDS, qry_rds)
  if(any(class(X_Variables) == "error")) {
    con_RDS <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")
    X_Variables <- dbGetQuery(conn = con_RDS, qry_rds)
  }
  
  X_Variables$Date <- as.Date(X_Variables$Date)
  
  if(NROW(X_Variables) > 0) {
    # Joining the fund position data with the potential indicator variable corresponding to the symbols they have in their book.
    data.set_Q_Ends <- data.set_Q_Ends %>%
                      # Enriching the ownership data with the div yields
                      dplyr::left_join(X_Variables, by = c("Symbol", "Q_Ends" = "Date"))
    
    N_org <- data.set_Q_Ends %>% dplyr::group_by(Q_Ends) %>% dplyr::summarise(n_org = n())
    
    res_tmp <- data.set_Q_Ends %>% dplyr::filter(!is.na(FunVar_1_FF_DIV_YLD))
    if(NROW(res_tmp) > 0) {
      res_DivYld <- res_tmp %>% 
        dplyr::mutate(DivPay = ifelse(FunVar_1_FF_DIV_YLD > 0, TRUE, FALSE),
                      Div_ApproxPayment = (0.01 * FunVar_1_FF_DIV_YLD) * HOLDING_tot * LastPrice,
                      PortMrktValue = HOLDING_tot * LastPrice) %>%
        dplyr::left_join(N_org, by = "Q_Ends") %>% dplyr::group_by(Q_Ends) %>%
        dplyr::summarise(Div_PortProportion  = sum(DivPay) / n(),
                         Div_PortProportion2 = sum(DivPay) / mean(n_org), # proportion based on the entire port size @ that Q_Ends
                         PortDiv = sum(Div_ApproxPayment, na.rm = TRUE),
                         PortYld = sum(Div_ApproxPayment, na.rm = TRUE)/sum(PortMrktValue, na.rm = TRUE),
                         Count_wo_NA = mean(n_org), Count_w_NA = n())
      
      res_DivYld$fund <- f
    } else{
      res_DivYld <- NULL
      cat(f, file = "NoDivYieldInFunds.txt", sep = "\n", append = TRUE)
    }
   
    
  } else {
    res_DivYld <- NULL
    cat(f, file = "NoDivYieldInFunds.txt", sep = "\n", append = TRUE)
    }
 
  
  browser()
  # Saving Results
  res <- list(Cap_Result = res_cap, Sector_Result = res_sector, 
              SIC_Result = res_SIC, Industry_Result = res_Ind,
              Dividend_Result = res_DivYld)
  
  results_debug[[j]] <- res
  #return(res)
}
#browser()
if(debug_flag) Results_finals <- results_debug

t_end <- Sys.time()
res_CAP    <- lapply(Results_finals, function(x) x$Cap_Result) %>% dplyr::bind_rows()
res_Sector <- lapply(Results_finals, function(x) x$Sector_Result) %>% dplyr::bind_rows()


hist(res_CAP$CAP_Expousure)
res_CAP_concentration <- res_CAP %>% dplyr::group_by(fund)  %>% dplyr::filter(REPORT_DATE == max(REPORT_DATE)) %>%
  dplyr::filter(CAP_Expousure > 0.8) %>% dplyr::ungroup()


t_start <- Sys.time()
#Results_finals2 <- vector(mode = "list", length = 4)
# Increment in K are done manually!!
Results_finals2[[k]] <- foreach(j = 1:2000, #length(debug_seq), #dim(funds_ids)[1],
                                .combine = list, .multicombine = TRUE,
                                .maxcombine = dim(funds_ids)[1] + 1, .export = NULL, .noexport= c('con_lcl','con_RDS'),
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
                             
                             data.set <-  matchPrices2(data.set_000, Prices = Prices)
                             if(NROW(data.set) == 0) {
                               data.set <- data.set_000 %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>%
                                 dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE"))
                               
                               cat(f, file = "NoPricesMatch4Funds.txt", sep = "\n", append = TRUE)
                             }
                             
                             Port_Size <- data.set %>% dplyr::group_by(REPORT_DATE) %>% dplyr::summarise(Count = n())
                             
                             
                             res_sector <- data.set %>%
                               #data.set_000 %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>%
                               #dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PriceDate = price_dates[which.min(abs(price_dates - REPORT_DATE))]) %>%
                               #dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE")) %>%
                               
                               dplyr::mutate(MrktVal = HOLDING * PRICE) %>%
                               dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PortVal_repDate = sum(MrktVal, na.rm = TRUE)) %>%
                               dplyr::group_by(REPORT_DATE, SECTOR_CODE) %>% dplyr::mutate(SectorExpousure = sum(MrktVal, na.rm = TRUE)/ PortVal_repDate) %>%
                               dplyr::summarise(SectorExposure = mean(SectorExpousure,na.rm = TRUE))
                             
                             
                             res_cap <- data.set %>%
                               #data.set_000 %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>% 
                               #dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PriceDate = price_dates[which.min(abs(price_dates - REPORT_DATE))]) %>%
                               #dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE")) %>%
                               dplyr::mutate(MrktVal = HOLDING * PRICE) %>%
                               dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PortVal_repDate = sum(MrktVal, na.rm = TRUE)) %>%
                               dplyr::group_by(REPORT_DATE, CAP_GROUP) %>% dplyr::mutate(CAP_Expousure = sum(MrktVal, na.rm = TRUE)/ PortVal_repDate) %>%
                               dplyr::summarise(CAP_Exposure = mean(CAP_Expousure,na.rm = TRUE))
                             
                             
                             res_SIC <- data.set %>%
                               #data.set_000 %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>%
                               #dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PriceDate = price_dates[which.min(abs(price_dates - REPORT_DATE))]) %>%
                               #dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE")) %>%
                               dplyr::mutate(MrktVal = HOLDING * PRICE) %>%
                               dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PortVal_repDate = sum(MrktVal, na.rm = TRUE)) %>%
                               dplyr::group_by(REPORT_DATE, PRIMARY_SIC_CODE) %>% dplyr::mutate(SIC_Expousure = sum(MrktVal, na.rm = TRUE)/ PortVal_repDate) %>%
                               dplyr::summarise(SIC_Exposure = mean(SIC_Expousure,na.rm = TRUE))
                             
                             
                             res_Ind <- data.set %>%
                               #data.set_000 %>% dplyr::mutate(REPORT_DATE = as.Date(REPORT_DATE), HOLDING = as.numeric(HOLDING)) %>% 
                               #dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PriceDate = price_dates[which.min(abs(price_dates - REPORT_DATE))]) %>%
                               #dplyr::left_join(Prices, by = c("FS_PERM_SEC_ID", "REPORT_DATE" = "PRICE_DATE")) %>%
                               dplyr::mutate(MrktVal = HOLDING * PRICE) %>%
                               dplyr::group_by(REPORT_DATE) %>% dplyr::mutate(PortVal_repDate = sum(MrktVal, na.rm = TRUE)) %>%
                               dplyr::group_by(REPORT_DATE, INDUSTRY_CODE) %>% dplyr::mutate(Industry_Expousure = sum(MrktVal, na.rm = TRUE)/ PortVal_repDate) %>%
                               dplyr::summarise(Industry_Exposure = mean(Industry_Expousure,na.rm = TRUE))
                             
                             
                             res_sector$fund <- res_cap$fund <- res_SIC$fund <- res_Ind$fund <- Port_Size$fund <- f
                             
                             # Dividend yield concentration analysis - bases on Q_Ends
                             #
                             # Aligning Report Date with Quarter Ends.
                             data.set_Q_Ends <- data.set %>% 
                               dplyr::mutate(Q_Ends = as.Date(as.character(timeDate::timeLastDayInQuarter(REPORT_DATE, format = "%Y-%m-%d", zone = "", FinCenter = ""))),
                                             HOLDING = as.numeric(HOLDING)) %>%
                               dplyr::group_by(FACTSET_FUND_ID, TICKER_EXCHANGE, Q_Ends) %>% 
                               dplyr::summarise(HOLDING_tot = sum(HOLDING, na.rm = TRUE),
                                                Symbol = dplyr::first(Symbol),
                                                CAP_GROUP = as.factor(dplyr::first(CAP_GROUP)),
                                                SECTOR_CODE = as.factor(dplyr::first(SECTOR_CODE)),
                                                LastPrice = dplyr::last(PRICE)
                               ) %>% dplyr::ungroup()
                             
                             # Getting unique Symbol and Date List
                             Symbol_list <- unique(data.set_Q_Ends$Symbol)
                             Date_list <- unique(data.set_Q_Ends$Q_Ends)
                             
                             # Getting Input data from RDS by querrying the 18 group tables: _gTable_01, ..., _gTable_18
                             rds_check <- dbGetQuery(conn = con_RDS, statement = "SELECT version();")
                             if(any(class(rds_check) == "error")) {
                               con_RDS <- dbConnect(RMySQL::MySQL(), default.file = "~/.my.cnf", group = "RDS")
                             }
                             # Getting div yield from FunVar_1_FF_DIV_YLD from RDS
                             qry_rds <- paste0("SELECT * FROM q4_intel.FunVar_1_FF_DIV_YLD where Symbol IN ('",
                                               paste(Symbol_list, collapse = "' , '"), "')",
                                               " AND Date IN ('", paste(Date_list, collapse = "' , '"), "');")
                             X_Variables <- dbGetQuery(conn = con_RDS, qry_rds)
                             if(any(class(X_Variables) == "error")) {
                               con_RDS <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")
                               X_Variables <- dbGetQuery(conn = con_RDS, qry_rds)
                             }
                             
                             X_Variables$Date <- as.Date(X_Variables$Date)
                             
                           if(NROW(X_Variables) > 0) {
                             # Joining the fund position data with the potential indicator variable corresponding to the symbols they have in their book.
                             data.set_Q_Ends <- data.set_Q_Ends %>%
                               # Enriching the ownership data with the div yields
                               dplyr::left_join(X_Variables, by = c("Symbol", "Q_Ends" = "Date"))
                             
                             N_org <- data.set_Q_Ends %>% dplyr::group_by(Q_Ends) %>% dplyr::summarise(n_org = n())
                             res_tmp <- data.set_Q_Ends %>% dplyr::filter(!is.na(FunVar_1_FF_DIV_YLD))
                             if(NROW(res_tmp) > 0) {
                               res_DivYld <- res_tmp %>% 
                                 dplyr::mutate(DivPay = ifelse(FunVar_1_FF_DIV_YLD > 0, TRUE, FALSE),
                                               Div_ApproxPayment = (0.01 * FunVar_1_FF_DIV_YLD) * HOLDING_tot * LastPrice,
                                               PortMrktValue = HOLDING_tot * LastPrice) %>%
                                 dplyr::left_join(N_org, by = "Q_Ends") %>% dplyr::group_by(Q_Ends) %>%
                                 dplyr::summarise(Div_PortProportion  = sum(DivPay) / n(),
                                                  Div_PortProportion2 = sum(DivPay) / mean(n_org), # proportion based on the entire port size @ that Q_Ends
                                                  PortDiv = sum(Div_ApproxPayment, na.rm = TRUE),
                                                  PortYld = sum(Div_ApproxPayment, na.rm = TRUE)/sum(PortMrktValue, na.rm = TRUE),
                                                  Count_wo_NA = mean(n_org), Count_w_NA = n())
                               
                               res_DivYld$fund <- f
                             } else {
                               res_DivYld <- NULL
                               cat(f, file = "NoDivYieldInFunds.txt", sep = "\n", append = TRUE)
                             }
                           } else {
                             res_DivYld <- NULL
                             cat(f, file = "NoDivYieldInFunds.txt", sep = "\n", append = TRUE)
                           }
                             
                             
                             # Saving Results
                             res <- list(Cap_Result = res_cap, Sector_Result = res_sector, 
                                         SIC_Result = res_SIC, Industry_Result = res_Ind,
                                         Dividend_Result = res_DivYld, Port_Size = Port_Size)
                             return(res)
                           }

t_end <- Sys.time()
saveRDS(Results_finals2, "Results_finals2_2_wip.rds")
#Results_finals2 <- readRDS("Results_finals2_2_wip.rds")


res_CAP <- res_Sector <- res_Industry <- res_Dividend <- res_PortSize <- c()

for(k in 1:4){
  res_CAP      <- rbind(res_CAP   ,   lapply(Results_finals2[[k]], function(x) x$Cap_Result) %>% dplyr::bind_rows())
  res_Sector   <- rbind(res_Sector,   lapply(Results_finals2[[k]], function(x) x$Sector_Result) %>% dplyr::bind_rows())
  res_SIC      <- rbind(res_SIC,      lapply(Results_finals2[[k]], function(x) x$SIC_Result) %>% dplyr::bind_rows())
  res_Industry <- rbind(res_Industry, lapply(Results_finals2[[k]], function(x) x$Industry_Result) %>% dplyr::bind_rows())
  res_Dividend  <- rbind(res_Dividend,lapply(Results_finals2[[k]], function(x) x$Dividend_Result) %>% dplyr::bind_rows())
  res_PortSize <- rbind(res_PortSize, lapply(Results_finals2[[k]], function(x) x$Port_Size) %>% dplyr::bind_rows())
}


filter_CountFund <- function(data.tbl, VarName, thresh, mostRecent = FALSE, CountRes = TRUE){
  if(mostRecent) data.tbl <- data.tbl %>% dplyr::group_by(fund) %>% dplyr::filter(REPORT_DATE == max(REPORT_DATE))
  else data.tbl <- data.tbl %>% dplyr::group_by(fund)
  crit_1 <- lazyeval::interp(~ VarName > thresh, .value = list(VarName = as.name(VarName), thresh = thresh))
  res_tmp <- data.tbl %>% dplyr::filter_(.dots = crit_1)
  if(CountRes) {
    count <- dplyr::distinct(res_tmp, fund, .keep_all = FALSE) %>% NROW()
    res <- list(data = res_tmp, count = count)
  } else res <- res_tmp
  
  return(res)
}


filter_CountFund_Q_Ends <- function(data.tbl, VarName, thresh, mostRecent = FALSE, CountRes = TRUE){
  if(mostRecent) data.tbl <- data.tbl %>% dplyr::group_by(fund) %>% dplyr::filter(Q_Ends == max(Q_Ends))
  else data.tbl <- data.tbl %>% dplyr::group_by(fund)
  crit_1 <- lazyeval::interp(~ VarName > thresh, .value = list(VarName = as.name(VarName), thresh = thresh))
  res_tmp <- data.tbl %>% dplyr::filter_(.dots = crit_1)
  if(CountRes) {
    count <- dplyr::distinct(res_tmp, fund, .keep_all = FALSE) %>% NROW()
    res <- list(data = res_tmp, count = count)
  } else res <- res_tmp
  
  return(res)
}


hist(res_CAP$CAP_Expousure, 100)
res_CAP_95_mostRecent <- filter_CountFund(data.tbl = res_CAP, VarName = "CAP_Expousure",
                                          thresh = 0.95, mostRecent = TRUE, CountRes = FALSE)
res_CAP_95_mostRecent_funds <- dplyr::semi_join(GoodFund, res_CAP_95_mostRecent, by = c("UniqueFund" = "fund"))

hist(res_Sector$SectorExpousure, 100)
res_SECTOR_95_mostRecent <- filter_CountFund(data.tbl = res_Sector, VarName = "SectorExpousure",
                                             thresh = 0.95, mostRecent = TRUE, CountRes = FALSE)
res_SECTOR_95_mostRecent_funds <- dplyr::semi_join(GoodFund, res_SECTOR_95_mostRecent, by = c("UniqueFund" = "fund"))

res_SECTOR_NAN_mostRecent <- res_Sector %>% dplyr::group_by(fund) %>% dplyr::filter(REPORT_DATE == max(REPORT_DATE)) %>%
  dplyr::filter(is.nan(SectorExpousure))
res_SECTOR_NAN_mostRecent_funds <- dplyr::semi_join(GoodFund, res_SECTOR_NAN_mostRecent, by = c("UniqueFund" = "fund"))


hist(res_Industry$Industry_Expousure, 100)
res_Industry_95_mostRecent <- filter_CountFund(data.tbl = res_Industry, VarName = "Industry_Expousure",
                                               thresh = 0.95, mostRecent = TRUE, CountRes = FALSE)
res_Industry_95_mostRecent_funds <- dplyr::semi_join(GoodFund, res_Industry_95_mostRecent, by = c("UniqueFund" = "fund"))

hist(res_SIC$SIC_Expousure, 100)
res_SIC_95_mostRecent <- filter_CountFund(data.tbl = res_SIC, VarName = "SIC_Expousure",
                                          thresh = 0.95, mostRecent = TRUE, CountRes = FALSE)
res_SIC_95_mostRecent_funds <- dplyr::semi_join(GoodFund, res_SIC_95_mostRecent, by = c("UniqueFund" = "fund"))

hist(res_Dividend$Div_PortProportion)
res_Div_95_mostRecent <- filter_CountFund_Q_Ends(data.tbl = res_Dividend, VarName = "Div_PortProportion2",
                                                 thresh = 0.999, mostRecent = TRUE, CountRes = FALSE)
res_Div_95_mostRecent_funds <- dplyr::semi_join(GoodFund, res_Div_95_mostRecent, by = c("UniqueFund" = "fund"))