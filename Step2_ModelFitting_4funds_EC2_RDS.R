# Helper Function
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

add_EntryExit <- function(x, Hld_dates_tbl) {
  if(NROW(x) != NROW(Hld_dates_tbl)) {
    res <- dplyr::full_join(x, Hld_dates_tbl, by = "Q_Ends") %>% dplyr::arrange(Q_Ends) %>% 
      dplyr::mutate(FACTSET_FUND_ID = na.omit(unique(FACTSET_FUND_ID)),
                    TICKER_EXCHANGE = na.omit(unique(TICKER_EXCHANGE)),
                    Symbol = na.omit(unique(Symbol)),
                    CAP_GROUP = na.omit(unique(CAP_GROUP)),
                    SECTOR_CODE = na.omit(unique(SECTOR_CODE)),
                    
                    # Enter_position = ifelse(!is.na(dplyr::lead(HOLDING_tot)) & !is.na(HOLDING_tot) &  is.na(lag(HOLDING_tot)), TRUE, FALSE),
                    Enter_position = ifelse(!is.na(HOLDING_tot) &  is.na(lag(HOLDING_tot)), TRUE, FALSE),
                    Exit_position  = ifelse( is.na(HOLDING_tot) & !is.na(lag(HOLDING_tot)), TRUE, FALSE),
                    # Exit_position  = ifelse( is.na(dplyr::lead(HOLDING_tot)) & is.na(HOLDING_tot) & !is.na(lag(HOLDING_tot)), TRUE, FALSE),
                    HOLDING_tot = ifelse(Exit_position & !is.na(lag(HOLDING_tot)), 0, HOLDING_tot),
                    HOLDING_tot = ifelse(is.na(HOLDING_tot) & !is.na(dplyr::lead(HOLDING_tot)), 0, HOLDING_tot)
      )
  } else {
    res <- x
    res$Enter_position <- res$Exit_position <- FALSE
  }
  # making the enter/exit variable a factor.
  res$Enter_position <- as.factor(res$Enter_position)
  res$Exit_position  <- as.factor(res$Exit_position)
  return(res)
}

na.locf_tbl <- function(my_tbl = Fundamental_data, dt_var = "Q_Ends") {
  require(xts)
  symbol = unique(my_tbl$Symbol)
  tbl_xts <- xts::xts(x = my_tbl[, !(names(my_tbl) %in% c("Q_Ends", "Symbol"))], order.by = my_tbl[[dt_var]]) %>% xts:::na.locf.xts()
  res <- data.frame(Symbol = symbol, Q_Ends = index(tbl_xts), coredata(tbl_xts) )
  return(res)
}

cleanMem <- function(n=10) { for (i in 1:n) gc() }

conformal_prediction_score <- function(model, new_data, details = FALSE) {
  # Computing variables for Comformal Prediction Confidence Intervals
  # See CalcualteCVScores, CalculatesPValues method in the conformal R-package
  # https://github.com/isidroc/conformal/edit/master/conformal/R/ConformalClassification.R
  
  # Computing the conformaty measure
  MondrianICP <- model$finalModel$votes
  MondrianICP <- apply(MondrianICP, 2, sort, decreasing=FALSE)
  NonconformityScoresMatrix <- MondrianICP
  
  # Calculating Confidence Level for class prediction
  pred <- predict(model$finalModel, newdata = new_data ,predict.all=TRUE) # individual or aggregate
  
  ntrees <- model$finalModel$ntree
  votes <- apply(pred$individual,1,function(x){table(x)})
  if(is.list(votes)) {
    out <- c()
    # As defined in the original code. However from testing we found that votes is a matrix and not a list as in the example given in the package
    for (i in colnames(NonconformityScoresMatrix)){
      out<-cbind(out,sapply(votes,function(x) x[i]))
    }
  } else out <- t(votes)
  
  out[is.na(out)] <- 0
  out_count <- out
  out <- out/ntrees
  colnames(out) <- colnames(NonconformityScoresMatrix)
  
  pval <- t(apply(out,1,function(x){ apply(do.call(rbind, lapply(as.data.frame(t(NonconformityScoresMatrix)), "<", x)),2,sum)    }))
  pval <- pval / nrow(NonconformityScoresMatrix)
  res_tmp <- as.data.frame(1 - pval) # Returning the 
  res_tmp$prediction <- pred$aggregate
  res_tmp <- cbind(res_tmp, out_count)
  if(details == TRUE)
    res <- list(NonconformityScores = NonconformityScoresMatrix,pVal = pval, res = res_tmp, prediction = pred, votes = votes, out_count)
  else res <- res_tmp
  return(res)
}


ls.S3 <- function(bkt = "q4quant", path = "_ModelResults/1_Entry/_ResultsList/"){
  
  res_tmp <- get_bucket(bucket = bkt, prefix = path)
  res <- data.frame(FileName = sapply(res_tmp, FUN = function(x){x$Key}), stringsAsFactors = FALSE)
  while(NROW(res) %% 1000 == 0) {
    res_tmp <- get_bucket(bucket = bkt, prefix = path, marker = res$FileName[NROW(res)])
    res_tmp2 <- data.frame(FileName = sapply(res_tmp, FUN = function(x){x$Key}), stringsAsFactors = FALSE)
    res <- rbind(res, res_tmp2)
  }
  res <- res %>% dplyr::mutate(FileName = gsub(pattern = path, x = FileName, replacement = "", fixed = TRUE)) %>%
         dplyr::filter(FileName != "")
  return(res)
}

# Creating MySQL connection locally ----
library(RMySQL)
# con <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel2")
con_lcl_master <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")
con_RDS_master <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")
# dbDisconnect(con)

# Setting Global variable (will be available for the workers too) -----
#
# 1. Getting US companies Fundamental Data
# Setting the time zone if it's not set
# https://www.r-bloggers.com/doing-away-with-%E2%80%9Cunknown-timezone%E2%80%9D-warnings/
if (Sys.getenv("TZ") == "") Sys.setenv(TZ="US/Eastern")
#Fundamental_data_0 <- dbReadTable(con, "fundamental_data_Q_Ends") %>% dplyr::mutate(Q_Ends = as.Date(Q_Ends))
#Fundamental_data <- Fundamental_data_0 %>% dplyr::group_by(Symbol) %>% dplyr::do(out = na.locf_tbl(.)) %$% dplyr::bind_rows(out)

# 2. Proportion of a fund holding data to be used for training the model
FRACTION_TRAINING <- 0.75

# 3. Minimum number of holding per funds. If funds has below that many holdings, it will not enter the modeling analysis
min_num_samples <- 20

# 3. Path to save fitted regression model for future use
save.model.path <- "C:/Users/Administrator/Documents/R_workspaces/AI_Targetting/ModelResults/"



#### Getting List of funds ID to process -----
# funds_ids <- dbReadTable(con, " fund_ids_us")
#### Funds_Ids for small samples
# funds_ids <- dbGetQuery(con, "SELECT DISTINCT FACTSET_FUND_ID FROM fund_us_holdings_hist_w_symbols_and_sic_SAMPLE")
#### Funds_Ids for medium samples (size = 1000)
funds_ids <- dbGetQuery(conn = con_lcl_master, "SELECT FACTSET_FUND_ID FROM fund_ids_us")

# Skipping Processed funds stored in S3.
library(aws.s3)
processedFunds <- ls.S3(bkt = "q4quant", path = "_ModelResults/1_Entry/_ResultsList/") %>%
                  dplyr::transmute(FACTSET_FUND_ID = gsub(x = FileName, pattern = "_Entry.rds", replacement = "",fixed = TRUE))

funds_ids <- dplyr::anti_join(funds_ids, processedFunds, by = "FACTSET_FUND_ID")

# Setting up parallel processing ------
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



# library(glmnet) # attached with foreach for each of the workers
library(caret)
library(randomForest)
library(RFinfer)
library(ROCR)
library(conformal)
library(aws.s3)
# Verbose - to be tested
verbose <- TRUE
# Debug funds
debug_flag <- FALSE
debug_funds <- c("04B8CT-E", "04BK4J-E")
debug_funds_bigTraining <- c("04B8L6-E","04BFN4-E", "04CT14-E", "04B9YF-E")
debug_funds_Morethan100_AUM <- c("04CC5R-E", "04B9GH-E", "04BFN4-E", "04B8FN-E", "04B9KG-E",
                                 "04B8VJ-E", "04B8NM-E", "04CDHZ-E", "04B927-E", "04B8K7-E")

debug_seq <- which(funds_ids$FACTSET_FUND_ID %in% 
                     #debug_funds_bigTraining
                     debug_funds_Morethan100_AUM
                   )


# Classification models are either "GLMNET" or RandomForest 
ClassificationModel <- "RandomForest"
# Sampling method to correct of class imbalance in the case of Entry/Exit. Possible Value: up, down
samplingMethod4EntryExit <- "down"
writeModelResults2Disk <- TRUE # FALSE
Save2S3 <- TRUE


# TO DO: Normalize and scale the features before fitting
t_start <- Sys.time()
Results_finals <- vector(mode = "list", length = 5)
for (i in 1:1) {
  ##  Looping through different dataset:
  # i = 1: Entry
  # i = 2: Change in Position
  # i = 3: Exit
  fitting_data <- switch(as.character(i),
                         "1" = "Entry",
                         "2" = "Position_change",
                         "3" = "Exit")
  
  print(paste("Modeling:", fitting_data))
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
  
  Results_finals[[i]] <- foreach(k = 1:dim(funds_ids)[1], #length(debug_seq), #
                                  .combine = list, .multicombine = TRUE,
                                  .maxcombine = dim(funds_ids)[1] + 1, .export = NULL, .noexport= c('con_lcl','con_RDS'), 
                                  .packages = c("caret","glmnet", "ROCR", "randomForest", "conformal", "aws.s3")) %dopar%  {

                                   #results_debug <- list()
                                   #for(k in 1:dim(funds_ids)[1]) { # browser()
                                   #browser()
                                   #j = debug_seq[k]
                                   j = k
                                   # Setting the seed for reproducability 
                                   set.seed(j)
                                    #Creating a list to save results
                                   res2ret <- list(Fund_ID = NULL, RegFit_warning = NULL, RegFit_error = NULL, Prob_Threshold = NULL, Results_coeff = NULL, Results_stat = NULL,
                                                   Insufficient_data = NULL, Training_N = NULL, Testing_N = NULL, fitting_data = fitting_data, Info.Msg = NULL,
                                                   IndicatorList = NULL, RelevantIndicatorList = NULL, ConfidenceLevels = NULL,
                                                   TrainingSamples = NULL)
                                   
                                   # Processing i_th fund
                                   res2ret$Fund_ID <- f <- funds_ids$FACTSET_FUND_ID[j]
                                   # res2ret$Fund_ID <- f <- fund2debug$FACTSET_FUND_ID[j]
                                   #browser()
                                   if(verbose == TRUE) {
                                     # Creating a node specific files to write debug info to
                                     fname <- paste(Sys.info()[['nodename']], Sys.getpid(), sep='-')
                                     # Creating a single (local) file to write diagnostic to:
                                     fname <- paste("Verbose_Log", Sys.Date(), ClassificationModel, samplingMethod4EntryExit, sep='-')
                                     # message(paste0("Processing fund:", f ,". Memory used: ", pryr::mem_used()))
                                     verbose_msg <- paste0(fitting_data, ", Worker: ", paste(Sys.time(), Sys.info()[['nodename']], Sys.getpid(), sep='-'),
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

                                   data.set_00 <- data.set_000 %>% 
                                                  dplyr::mutate(Q_Ends = as.Date(as.character(timeDate::timeLastDayInQuarter(REPORT_DATE, format = "%Y-%m-%d", zone = "", FinCenter = ""))),
                                                                HOLDING = as.numeric(HOLDING)) %>%
                                                  dplyr::group_by(FACTSET_FUND_ID, TICKER_EXCHANGE, Q_Ends) %>% 
                                                  dplyr::summarise(HOLDING_tot = sum(HOLDING, na.rm = TRUE),
                                                                   Symbol = dplyr::first(Symbol),
                                                                   CAP_GROUP = as.factor(dplyr::first(CAP_GROUP)),
                                                                   SECTOR_CODE = as.factor(dplyr::first(SECTOR_CODE))
                                                  ) %>% dplyr::ungroup()
                                   
                                   NROW(data.set_00) #  95160 less than NROW(data.set_000) since we aggregated to quarterly.
                                   
                                   fund_hld_dates_tbl <- dplyr::distinct(data.set_00, Q_Ends)
                                   data.set_0 <- data.set_00 %>% dplyr::group_by(FACTSET_FUND_ID, TICKER_EXCHANGE) %>% 
                                                 dplyr::do(res = add_EntryExit(., Hld_dates_tbl = fund_hld_dates_tbl)) %$%
                                                 dplyr::bind_rows(res)  %>% dplyr::filter(!is.na(HOLDING_tot))
                                   
                                   NROW(data.set_0) # 98459
                                   
                                   Symbol_list <- unique(data.set_0$Symbol)
                                   Date_list <- unique(data.set_0$Q_Ends)
                                   
                                   # Getting Input data from RDS by querrying the 18 group tables: _gTable_01, ..., _gTable_18
                                   rds_check <- dbGetQuery(conn = con_RDS, statement = "SELECT version();")
                                   if(any(class(rds_check) == "error")) {
                                     con_RDS <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")
                                   }
                                   for(tbl_num in 1:18){
                                     qry_rds <- paste0("Select * from _gTable_", formatC(tbl_num, digits = 2, width = 2, flag = 0),
                                                  " where Symbol IN ('", paste(Symbol_list, collapse = "' , '"), "')",
                                                  " AND Date IN ('", paste(Date_list, collapse = "' , '"), "');")
                                     X_Variables_tmp <- dbGetQuery(conn = con_RDS, qry_rds)
                                     if(any(class(X_Variables_tmp) == "error")) {
                                       con_RDS <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")
                                       X_Variables_tmp <- dbGetQuery(conn = con_RDS, qry_rds)
                                     }
                                     if(tbl_num == 1) X_Variables <- X_Variables_tmp
                                     else X_Variables <- dplyr::left_join(X_Variables, X_Variables_tmp, by = c("Date", "Symbol"))
                                   }
                                   
                                   X_Variables$Date <- as.Date(X_Variables$Date)
                                   
                                   
                                   
                                   # Joining the fund position data with the potential indicator variable corresponding to the symbols they have in their book.
                                   data_set_0 <- data.set_0 %>%
                                                     # Enriching the ownership data with the Indicator Variables
                                                     dplyr::left_join(X_Variables, by = c("Symbol", "Q_Ends" = "Date")) %>%
                                                     dplyr::group_by(TICKER_EXCHANGE) %>% dplyr::arrange(Q_Ends) %>%
                                                     dplyr::mutate(# HOLDING_tot_lag = lag(HOLDING_tot),
                                                                   HOLDING_chng = HOLDING_tot - lag(HOLDING_tot),
                                                                   Buy_Sell = as.factor(ifelse(HOLDING_chng < 0, "Sell", "Buy"))) %>%
                                                     dplyr::ungroup() %>% dplyr::arrange(Symbol, Q_Ends) %>% 
                                                     dplyr::select(-FACTSET_FUND_ID, -TICKER_EXCHANGE, # -Symbol, -Q_Ends,
                                                                   -HOLDING_tot, -HOLDING_chng,
                                                                   -CAP_GROUP, -SECTOR_CODE
                                                                   # -Enter_position, -Exit_position
                                                                   ) %>%
                                                      dplyr::filter(!is.na(Buy_Sell))
                                   
                                   
                                   
                                  
                                   data.set_2process <- switch(as.character(i), 
                                                      "1" = data_set_0 %>% dplyr::select(-Exit_position, -Buy_Sell),
                                                      "2" = data_set_0 %>% dplyr::select(-Exit_position, -Enter_position),
                                                      "3" = data_set_0 %>% dplyr::select(-Buy_Sell, -Enter_position)
                                   )
                                   
                                   varLen <- sapply(data.set_2process, function(x) length(which(!is.na(x))))
                                   varLen <- data.frame(Variable = names(varLen), varLen)
                                   # View(varLen)
                                   
                                   # Feature Selection based on Data availability
                                   pLossData <- 0.05
                                   pLossData_incr <- 0.01
                                   retentionRatio <- 1
                                   
                                   num_var2consider <- NULL
                                   min_num_var2consider <- 100
                                   
                                   if(NROW(data.set_2process) == 0){
                                     res2ret$Insufficient_data <- TRUE
                                     res2ret$Info.Msg <- paste("This fund has no data to model for the case of", fitting_data)
                                     ifelse(debug_flag, next, {
                                       if(Save2S3 == TRUE)
                                         s3saveRDS(x = res2ret, bucket = "q4quant", object = paste0(S3_path, "_ResultsList/",f, "_", fitting_data, ".rds"))
                                       return(res2ret)
                                       })
                                   }
                                   
                                   while(is.null(num_var2consider) || (num_var2consider < min_num_var2consider) & retentionRatio > 0.5) {
                                     pLossData <- pLossData + pLossData_incr
                                     
                                     VarLen_reduced <- dplyr::filter(varLen, varLen > (1 - pLossData)*max(varLen)) %>% dplyr::select(Variable) %>%
                                                       dplyr::filter(!(Variable %in% c("Symbol", "Q_Ends", "HOLDING_tot", "HOLDING_chng"))) %>% unlist()
                                     
                                     data_set_reduced <- data.set_2process[, VarLen_reduced] %>% na.omit()
                                     retentionRatio <- nrow(data_set_reduced)/nrow(data.set_2process)
                                     num_var2consider <- length(VarLen_reduced)
                                   }
                                   
                                   res2ret$IndicatorList <- VarLen_reduced
                                   
                                   
                                   #browser()
                                   Y_Var <- switch(as.character(i),
                                                   "1" = "Enter_position",
                                                   "2" = "Buy_Sell",
                                                   "3" = "Exit_position")
                                   
                                   X_Var <-  setdiff(VarLen_reduced, Y_Var)
                                   
                                   # Further Feature Selection based on Information gain
                                   data_set4info.gain <- data_set_reduced[, c(Y_Var, X_Var)]
                                   
                                   info.gain_formula <- as.formula(paste(Y_Var, "~ ."))
                                   info_gain <- FSelectorRcpp::information_gain(formula = info.gain_formula, data = data_set4info.gain, type = "infogain") %>% 
                                                tibble::rownames_to_column(var = "PotentialVariable") # %>% dplyr::filter(importance != 0)
                                   
                                   if(all(info_gain$importance == 0)) {
                                     # If there none of the variable show significance, we will proceed with the initial set, and let the fitting algoritm decide
                                     X_Var <- X_Var
                                     res2ret$Info.Msg <- "No Information gain from any of the potential variables"
                                     } else {
                                       X_Var <- info_gain$PotentialVariable
                                       res2ret$RelevantIndicatorList <- info_gain$PotentialVariable
                                     }
                                   
                                   data.set <- data_set4info.gain[, c(Y_Var, X_Var)]
                                   
                                   # Cleaning the workspace and memory 
                                   rm(X_Variables, X_Variables_tmp, data_set4info.gain, data_set_reduced, data.set_2process, data_set_0, data.set_00, data.set_000)
                                   cleanMem()
                                   #browser()
                                   
                                   #if(j == 456) browser() 
                                   # Skip fund if there is less than
                                   if(NROW(data.set) < min_num_samples) {
                                     res2ret$Insufficient_data <- TRUE
                                     ifelse(debug_flag, next, {
                                       if(Save2S3 == TRUE)
                                         s3saveRDS(x = res2ret, bucket = "q4quant", object = paste0(S3_path, "_ResultsList/",f, "_", fitting_data, ".rds"))
                                       return(res2ret)
                                      })
                                   } else res2ret$Insufficient_data <- FALSE
                                   
                                   
                                   # Splitting the data_set into training and testing set based on proportion variable
                                   # Possible alternative is to use the createDataPartition function in the caret package
                                   sample_ids <- res2ret$TrainingSamples <- createDataPartition(data.set[[Y_Var]], p = FRACTION_TRAINING,
                                                                                                list = FALSE, times = 1)
                                   Training_data <- data.set[sample_ids , c(Y_Var, X_Var)]
                                   
                                   # Checking to see if there is more than 1 level for factor variable in the training set.
                                   # If a factor variable takes only 1 variable, it's dropped from the regression
                                   single_value_check <- sapply(Training_data, function(x) length(unique(x)) == 1) %>% which(. == TRUE)
                                   if (length(single_value_check) > 0) {
                                     X_Var <-  setdiff(X_Var, names(single_value_check))
                                     Training_data <- data.set[sample_ids , c(Y_Var, X_Var)]
                                   }
                                   Test_data     <- data.set[-sample_ids, c(Y_Var, X_Var)]
                                   
                                   res2ret$Training_N <- NROW(Training_data)
                                   res2ret$Testing_N  <- NROW(Test_data)
                                   
                                   
                                   X <- model.matrix( ~ .-1, data = Training_data[, X_Var])
                                   Y <- as.numeric(Training_data[[Y_Var]])
                                   
                                   
                                   # Cross validation for hyper-parameter estimation
                                   N_folds <- ifelse(res2ret$Training_N < 500, 3, 5)
                                   
                                   
                                   # Modeling using the Caret package
                                   # http://amunategui.github.io/dummyVar-Walkthrough/
                                   X_caret_dummy <- dummyVars( ~ .-1 , data = data.set[, X_Var], fullRank = FALSE)
                                   X_caret <- data.frame(predict(X_caret_dummy, newdata = Training_data[, X_Var]))
                                   Y_caret <- Training_data[[Y_Var]]
                                   
                                   objControl <- trainControl(method='cv', number = N_folds, 
                                                              # returnResamp='none', 
                                                              #savePredictions = TRUE,
                                                              # Changed to the below value for conformal prediction.
                                                              returnResamp = "all",
                                                              savePredictions = TRUE)
                                   
                                   # Upsampling the entry/exit obserrvation when trainning the classification model
                                   # http://dpmartin42.github.io/blogposts/r/imbalanced-classes-part-1
                                   if(i %in% c(1,3)) objControl$sampling <- samplingMethod4EntryExit
                                   
                                   glmnet_lasso <- tryCatch(
                                     # http://stackoverflow.com/questions/12193779/how-to-write-trycatch-in-r
                                     # Try part
                                     expr = { switch(toupper(ClassificationModel),
                                                     # GLMNet 
                                                     "GLMNET" =  train(X_caret, Y_caret, method='glmnet', metric = "Accuracy", trControl=objControl, maxit=500000,
                                                                       tuneGrid = expand.grid(.alpha = c(0, 0.005, 0.025, seq(.05, 1, length = 15),  0.975, 1),
                                                                                              .lambda = c(exp(seq(log(1e-5), log(1e0), length.out = 20)), 10^(1:5)))),
                                                     
                                                     # Random Forest Modeling
                                                     "RANDOMFOREST" =  train(X_caret, Y_caret, method='rf', metric = "Accuracy", trControl=objControl, ntree = 501,
                                                                             ## Tell randomForest to sample by strata.. That means within each class
                                                                             strata = Y_caret, sampsize = rep(min(table(Y_caret)), 2),
                                                                             tuneGrid = expand.grid(.mtry = c(1, 3, 5, 7, 10)),
                                                                             # parameters value for comformal prediction
                                                                             keep.forest=TRUE, predict.all=TRUE, norm.votes=TRUE)
                                     )
                                       
                                       
                                     },
                                     error = function(cond) {
                                       res2ret$RegFit_error <<- TRUE
                                       res2ret$Error_Regression <<- cond
                                       # return(res2ret)
                                     },
                                     warning = function(cond){
                                       res2ret$RegFit_warning <<- TRUE
                                       res2ret$Warning_Regression <<- cond
                                       # return(res2ret)
                                     }
                                   )
                                   # browser()
                                   if(any(class(glmnet_lasso) %in% c("warning", "error"))) {
                                     #Results_final[[counter]] <- res2ret
                                     ifelse(debug_flag, next, {
                                       if(Save2S3 == TRUE) s3saveRDS(x = res2ret, bucket = "q4quant", object = paste0(S3_path, "_ResultsList/",f, "_", fitting_data, ".rds"))
                                       return(res2ret)
                                       })
                                   } else {
                                     res2ret$RegFit_warning <- res2ret$RegFit_error <- FALSE
                                     
                                     # Saving model to disk for future use
                                     #
                                     if(writeModelResults2Disk) {
                                       if(Save2S3 == TRUE) {
                                         # Saving to S3 buket
                                         s3saveRDS(x = glmnet_lasso, bucket = "q4quant",
                                                   object = paste0(S3_path, f, "_", fitting_data, ".rds")
                                                   )
                                          
                                       } else {
                                         # Writing to disk
                                         outCon <- file(paste0(save.model.path,f,".txt"), "w")
                                         model.char <- rawToChar(serialize(glmnet_lasso, NULL, ascii=T))
                                         cat(model.char, file=outCon); close(outCon)
                                       }
                                     }
                                     
                                     # Determine the optimal probability thereshold value to determine the class (buy vs sell)
                                     # https://hopstat.wordpress.com/2014/12/19/a-small-introduction-to-the-rocr-package/
                                     # Logit model
                                     # pred_logit <- prediction(predictions = predict(glmnet_lasso,
                                     #                                                newx = model.matrix( ~ .-1, data = Training_data[, X_Var]),
                                     #                                                s = "lambda.min", type = "response"), 
                                     #                          labels = Training_data[, Y_Var])
                                     
                                     # Prediction for Caret output - notice that we are selecting the 2nd columns
                                     # http://amunategui.github.io/binary-outcome-modeling/
                                     pred_logit <- prediction(predictions = predict(object=glmnet_lasso,
                                                                                    X_caret, type = "prob")[[2]],
                                                              labels = Training_data[, Y_Var])
                                     
                                     # Equal cost for FP and FN
                                     cost.perf  <- performance(pred_logit, "cost", cost.fp = 1, cost.fn = 1)
                                     opt_threshold <- res2ret$Prob_Threshold <- pred_logit@cutoffs[[1]][which.min(cost.perf@y.values[[1]])]
                                     
                                     # Summarizing the model fit on Test Data only if the threshold is finite! -----
                                     # Determining Predictive accuracy -----
                                     #
                                     #
                                     X_test <- model.matrix( ~ .-1, data = Test_data[, X_Var])
                                     X_test_caret <- data.frame(predict(X_caret_dummy, newdata = Test_data[, X_Var]))
                                     
                                     # Conformal prediction - Confidence level
                                     res2ret$ConfidenceLevels <- conformal_prediction_score(glmnet_lasso, new_data = X_test_caret)
                                     res2ret$ConfidenceLevels$obs <- Test_data[[Y_Var]]
                                     
                                     
                                     
                                     #browser()
                                     
                                     if(!is.infinite(opt_threshold)) {
                                       Test_data$Y_prob <-predict(object=glmnet_lasso, X_test_caret, type = "prob")[[2]]
                                       Test_data$pred_glmnet_lasso_Buy_Sell <- ifelse(Test_data$Y_prob > opt_threshold, "Sell", "Buy")
                                     } else {
                                       Test_data$pred_glmnet_lasso_Buy_Sell <- predict(object=glmnet_lasso, X_test_caret, type = "raw")
                                     }
                                     
                                     # Mapping the variable name and its value to have 1 consistent evaluation function
                                     
                                     if(i %in% c(1,3)) {
                                       Test_data$Buy_Sell <- Test_data[[Y_Var]]
                                       Test_data[which(Test_data$Buy_Sell == TRUE ), "Buy_Sell"] <- "Sell"
                                       Test_data[which(Test_data$Buy_Sell == FALSE), "Buy_Sell"] <- "Buy"
                                     }
                                     
                                     # Creating Results data.frame -------
                                     Results <- Test_data %>%
                                       dplyr::mutate(
                                         # Predicted Buy/Sell given Buy/Sell
                                         pred_Buy_given_Buy_Lasso    = ifelse(pred_glmnet_lasso_Buy_Sell == "Buy"  & Buy_Sell == "Buy" , TRUE, FALSE),
                                         pred_Buy_given_Sell_Lasso   = ifelse(pred_glmnet_lasso_Buy_Sell == "Buy"  & Buy_Sell == "Sell", TRUE, FALSE),
                                         pred_Sell_given_Buy_Lasso   = ifelse(pred_glmnet_lasso_Buy_Sell == "Sell" & Buy_Sell == "Buy" , TRUE, FALSE),
                                         pred_Sell_given_Sell_Lasso  = ifelse(pred_glmnet_lasso_Buy_Sell == "Sell" & Buy_Sell == "Sell", TRUE, FALSE),
                                         ## Buy/Sell given Predicted Buy/Sell
                                         Buy_given_Pred_Buy_Lasso   = ifelse(Buy_Sell == "Buy"  & pred_glmnet_lasso_Buy_Sell == "Buy" , TRUE, FALSE),
                                         Buy_given_Pred_Sell_Lasso  = ifelse(Buy_Sell == "Buy"  & pred_glmnet_lasso_Buy_Sell == "Sell", TRUE, FALSE),
                                         Sell_given_Pred_Buy_Lasso  = ifelse(Buy_Sell == "Sell" & pred_glmnet_lasso_Buy_Sell == "Buy" , TRUE, FALSE),
                                         Sell_given_Pred_Sell_Lasso = ifelse(Buy_Sell == "Sell" & pred_glmnet_lasso_Buy_Sell == "Sell", TRUE, FALSE),
                                         # Accuracy
                                         Correct_pred_GLMnet_Lasso   = ifelse(pred_glmnet_lasso_Buy_Sell == Buy_Sell, TRUE, FALSE)) %>%
                                       dplyr::summarise(
                                         # TestData statistics
                                         Count_Test = as.integer(n()),
                                         Count_Buy  = as.integer(sum(Buy_Sell == "Buy")),
                                         Count_Sell = as.integer(sum(Buy_Sell == "Sell")),
                                         Percent_buy = paste(round(100 * Count_Buy/Count_Test, digits = 2), "%"),
                                         Percent_sell =  paste(round(100 * Count_Sell/Count_Test,digits = 2), "%"),
                                         # GLMnet Lasso
                                         # ============
                                         #
                                         # Predicted Buy/Sell given Buy/Sell
                                         Prob_pred_Buy_given_Buy_Lasso   = paste(round(100 * sum(pred_Buy_given_Buy_Lasso == TRUE)  /Count_Buy,digits = 2),"%"),
                                         Prob_pred_Buy_given_Sell_Lasso  = paste(round(100 * sum(pred_Buy_given_Sell_Lasso == TRUE) /Count_Sell,digits = 2),"%"),
                                         Prob_pred_Sell_given_Buy_Lasso  = paste(round(100 * sum(pred_Sell_given_Buy_Lasso == TRUE) /Count_Buy,digits = 2),"%"),
                                         Prob_pred_Sell_given_Sell_Lasso = paste(round(100 * sum(pred_Sell_given_Sell_Lasso == TRUE)/Count_Sell,digits = 2),"%"),
                                         # Buy/Sell given Predicted Buy/Sell
                                         Prob_Buy_given_pred_Buy_Lasso   = paste(round(100 * sum(Buy_given_Pred_Buy_Lasso == TRUE)  /sum(pred_glmnet_lasso_Buy_Sell == "Buy" ),digits = 2),"%"),
                                         Prob_Buy_given_pred_Sell_Lasso  = paste(round(100 * sum(Buy_given_Pred_Sell_Lasso == TRUE) /sum(pred_glmnet_lasso_Buy_Sell == "Sell"),digits = 2),"%"),
                                         Prob_Sell_given_pred_Buy_Lasso  = paste(round(100 * sum(Sell_given_Pred_Buy_Lasso == TRUE) /sum(pred_glmnet_lasso_Buy_Sell == "Buy" ),digits = 2),"%"),
                                         Prob_Sell_given_pred_Sell_Lasso = paste(round(100 * sum(Sell_given_Pred_Sell_Lasso == TRUE)/sum(pred_glmnet_lasso_Buy_Sell == "Sell"),digits = 2),"%"),
                                         # Accuracy
                                         Pred_Accuracy_GLMnet_Lasso      = paste(round(100 * sum(Correct_pred_GLMnet_Lasso == TRUE) /Count_Test,digits = 2),"%")) %>%
                                       as.vector()
                                     
                                     # Additional info for diagnostic ----
                                     #
                                     # Determining the number of independent variables
                                     # num_non_zero_coeff <- length(which(coef(glmnet_lasso, s = "lambda.min") != 0))
                                     
                                     if(toupper(ClassificationModel) == "GLMNET") {
                                       num_non_zero_coeff <- length(which(as.vector(coef(glmnet_lasso$finalModel,  glmnet_lasso$bestTune$lambda)) != 0))
                                     } else {num_non_zero_coeff <- NA}
                                     #
                                     # Saving Results statistics
                                     if(toupper(ClassificationModel) == "GLMNET") {
                                       res2ret$Results_stat <- Results <- cbind(fund = f, Non_Zero_Coeff = num_non_zero_coeff, Count_Training = dim(X)[1],
                                                                                Alpha = glmnet_lasso$bestTune$alpha, Lambda = glmnet_lasso$bestTune$lambda, Results)
                                       
                                     } else { 
                                       res2ret$Results_stat <- Results <- cbind(fund = f, Non_Zero_Coeff = num_non_zero_coeff, Count_Training = dim(X)[1],
                                                                                Num_FeaturesPerSplit = glmnet_lasso$bestTune$mtry, Results)
                                     }
                                     # Saving Model Coefficients
                                     # 
                                     # res2ret$Results_coeff <- data.frame(coef.name = dimnames(coef(glmnet_lasso, s = "lambda.min"))[[1]],
                                     #                                     coef.value = matrix(coef(glmnet_lasso, s = "lambda.min")))
                                     if(toupper(ClassificationModel) == "GLMNET") {
                                       res2ret$Results_coeff <- data.frame(coef.name = dimnames(coef(glmnet_lasso$finalModel,glmnet_lasso$bestTune$lambda))[[1]],
                                                                           coef.value = matrix(coef(glmnet_lasso$finalModel, glmnet_lasso$bestTune$lambda)))
                                       names(res2ret$Results_coeff)[2] <- paste(f, "CoeffValue", sep = "_")
                                     } else {
                                       res2ret$Results_coeff <- data.frame(coef.name = dimnames(varImp(glmnet_lasso, scale=FALSE)$importance)[[1]],
                                                                           Importance = varImp(glmnet_lasso, scale=FALSE)$importance)
                                       names(res2ret$Results_coeff)[2] <- paste(f, "importance", sep = "_")
                                     }
                                     
                                     
                                   }
                                   # removing clutter ----
                                   rm(glmnet_lasso)
                                   if(!debug_flag) {
                                     if(Save2S3 == TRUE) {
                                       s3saveRDS(x = res2ret, bucket = "q4quant", object = paste0(S3_path, "_ResultsList/",f, "_", fitting_data, ".rds"))
                                       if(verbose == TRUE) {
                                         # Creating a single (local) file to write diagnostic to:
                                         fname <- paste("ModelResults_Log", Sys.Date(), ClassificationModel, samplingMethod4EntryExit, sep='-')
                                         # message(paste0("Processing fund:", f ,". Memory used: ", pryr::mem_used()))
                                         verbose_msg <- paste0(fitting_data, ", Worker: ", paste(Sys.time(), Sys.info()[['nodename']], Sys.getpid(), sep='-'),
                                                               ", j = ", j,", wrote results list of fund:", f ,", to S3")
                                         cat(verbose_msg, file = paste0(fname, ".txt"), sep = "\n", append = TRUE)
                                         # print(verbose_msg)
                                       }
                                     }
                                     return(res2ret)
                                   } else {results_debug[[k]] <- res2ret}
                                 }
  #browser()
  if(debug_flag) Results_finals[[i]] <- results_debug
}

t_end <- Sys.time()
Results_finals[[4]] <- ClassificationModel
Results_finals[[5]] <- samplingMethod4EntryExit
names(Results_finals) <- c("Entry", "Position_Change", "Exit", "Fitting_Algorithm", "SamplingMethod4EntryExit")


# Closing MySQL connections in workers & stoping Cluster
clusterEvalQ(cl, {
  dbDisconnect(con_lcl)
  dbDisconnect(con_RDS)
  rm(con_lcl, con_RDS)
})
stopCluster(cl)
rm(cl)

clusterEvalQ(cl, {
  ls()
})