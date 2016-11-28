library(RMySQL)
# dbDisconnect(con)
con <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel2")


# Setting up parallel processing:
library(parallel)
# Calculate the number of cores
no_cores <- detectCores() - 1
library(foreach)
library(doParallel)

cl<-makeCluster(no_cores)
registerDoParallel(cl)
getDoParWorkers()
clusterExport(cl, "a")
# Starting the foreach
foreach(i = 1:length(Funds_IDs_sample), .combine = rbind #,
        #.export = "Funds_IDs_sample"
        ) %do% { # %dopar%  {
          fund = Funds_IDs_sample[i]
          qry <- paste0("SELECT * FROM fund_us_holdings_hist_w_symbols_and_sic where FACTSET_FUND_ID = '",fund , "'")
          data.set <- dbGetQuery(qry)
        }
stopCluster(cl)

# package neeeded for the analysis
# library(pROC) # used to find probablity threshold to determine the class
library(ROCR)  # used to find probablity threshold to determine the class
# Specifying the cost function for the pROC
# cost_fn <- function(analysis, sensitivity_cost = 1, specificity_cost = 1) {
#  res <- (sensitivity_cost * analysis$sensitivities) + (specificity_cost * analysis$specificities)
#  return(res)
#}

tbls_list <- dbListTables(con)
Fundamental_data <- dbReadTable(con, "fundamental_data_Q_Ends")
# funds_ids <- readRDS("Funds_Ids.rds")
funds_ids <- dbGetQuery(con, "SELECT DISTINCT FACTSET_FUND_ID FROM fund_us_holdings_hist_w_symbols_and_sic")
counter <- 0  # about 49 tables to process
FRACTION_TRAINING <- 0.75

save.model.path <- "./ModelResults/"
Results_final <- list()
for(f in funds_ids$FACTSET_FUND_ID) {
  counter <<- counter + 1
  message(paste0("Processing fund:", f ,". Memory used: ", pryr::mem_used()))
  qry <- paste0("SELECT * FROM fund_us_holdings_hist_w_symbols_and_sic where FACTSET_FUND_ID = '",f , "'")
  data.set <- dbGetQuery(con, qry)
  data.set <- data.set %>% 
    dplyr::mutate(Q_Ends = as.Date(timeDate::timeLastDayInQuarter(REPORT_DATE, format = "%Y-%m-%d", zone = "", FinCenter = "")),
                  HOLDING = as.numeric(HOLDING)) %>%
    dplyr::group_by(FACTSET_FUND_ID, TICKER_EXCHANGE, Q_Ends) %>% 
    dplyr::summarise(HOLDING_tot = sum(HOLDING, na.rm = TRUE),
                     Symbol = dplyr::first(Symbol),
                     CAP_GROUP = as.factor(dplyr::first(CAP_GROUP)),
                     SECTOR_CODE = as.factor(dplyr::first(SECTOR_CODE))
    ) %>% dplyr::ungroup() %>%
    # Enriching the ownership data with the fundamental data of the company
    dplyr::left_join(Fundamental_data, by = c("Symbol", "Q_Ends")) %>%
    dplyr::group_by(Symbol) %>% dplyr::arrange(Q_Ends) %>%
    dplyr::mutate(HOLDING_chng = HOLDING_tot - lag(HOLDING_tot),
                  Buy_Sell = as.factor(ifelse(HOLDING_chng < 0, "Sell", "Buy"))) %>%
    dplyr::ungroup() %>% dplyr::arrange(Symbol, Q_Ends)
    
  
  # Specifying the regression variables
  data.set <- na.omit(data.set) %>% 
              # Eliminating context (uneccessary) variable
              dplyr::select(-FACTSET_FUND_ID, -TICKER_EXCHANGE, -Q_Ends, -HOLDING_tot, -Symbol)
  
  Y_Var <- "Buy_Sell"
  X_Var <-  setdiff(names(data.set), Y_Var)

  # Splitting the data_set into training and testing set based on proportion variable
  n_samples <- floor(NROW(data.set) * FRACTION_TRAINING)
  sample_ids <- sample.int(n = NROW(data.set), size = n_samples, replace = FALSE)
  Training_data <- data.set[sample_ids , c(Y_Var, X_Var)]
  Test_data     <- data.set[-sample_ids, c(Y_Var, X_Var)]
  
  X <- model.matrix( ~ .-1, data = Training_data[, X_Var])
  Y <- as.numeric(Training_data[[Y_Var]])
  
  # Cross validation for hyper-parameter estimation
  N_folds <- 10
  fold_id <- sample(1:N_folds,size=length(Y),replace=TRUE)
  glmnet_lasso <- glmnet::cv.glmnet(X,Y, intercept=FALSE, foldid=fold_id, alpha=1, family = "binomial", type.measure="class") ## lasso regression - Sparse coeff
  # Determine the optimal probability thereshold value to determine the class (buy vs sell)
  # https://hopstat.wordpress.com/2014/12/19/a-small-introduction-to-the-rocr-package/
  # Logit model
  pred_logit <- prediction(predictions = predict(glmnet_lasso,
                                                 newx = model.matrix( ~ .-1, data = Training_data[, X_Var]),
                                                 # as.matrix(Training_data[,which(names(Training_data_classification) != "Buy_Sell")]),
                                                 s = "lambda.min", type = "response"), 
                           labels = Training_data[, Y_Var])
  # Equal cost for FP and FN
  cost.perf  <- performance(pred_logit, "cost", cost.fp = 1, cost.fn = 1)
  opt_threshold <- pred_logit@cutoffs[[1]][which.min(cost.perf@y.values[[1]])]
  
  # Determining Predictive accuracy -----
  browser()
  X_test <- model.matrix( ~ .-1, data = Test_data[, X_Var])
  Test_data$Y_prob <- predict(glmnet_lasso, s='lambda.min', newx=X_test, type="response")  %>% as.vector()
  Test_data$pred_glmnet_lasso_Buy_Sell <- ifelse(Test_data$Y_prob > opt_threshold, "Sell", "Buy")
  # Creating Results data.frame
  Results <- Test_data %>% dplyr::mutate(
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
            Correct_pred_GLMnet_Lasso   = ifelse(pred_glmnet_lasso_Buy_Sell == Buy_Sell, TRUE, FALSE)
  ) %>% dplyr::summarise(
            # TestData statistics
            Count = as.integer(n()),
            Count_Buy  = as.integer(sum(Buy_Sell == "Buy")),
            Count_Sell = as.integer(sum(Buy_Sell == "Sell")),
            Percent_buy = paste(round(100 * Count_Buy/Count, digits = 2), "%"),
            Percent_sell =  paste(round(100 * Count_Sell/Count,digits = 2), "%"),
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
            Pred_Accuracy_GLMnet_Lasso      = paste(round(100 * sum(Correct_pred_GLMnet_Lasso == TRUE) /Count,digits = 2),"%")) %>% as.vector()
  # Additional info for diagnostic
  # 
  # Looking at model coefficient
  coeff_lasso_df <- data.frame(coef.name = dimnames(coef(m_cv_glmnet_lasso, s = "lambda.min"))[[1]],
                               coef.value = matrix(coef(m_cv_glmnet_lasso, s = "lambda.min")))
  num_non_zero_coeff <- NROW(dplyr::filter(coeff_lasso_df,coef.value != 0))
  # Saving info to results
  Results <- cbind(fund = f, Non_Zero_Coeff = num_non_zero_coeff, Results)
  # Saving model to disk for future use
  outCon <- file(paste0(save.model.path,f,".txt"), "w")
  model.char <- rawToChar(serialize(glmnet_lasso, NULL, ascii=T))
  cat(model.char, file=outCon); close(outCon)
  # To read the model back in:
  # fname <- paste0(save.model.path,f,".txt")
  # model_fit <- unserialize(charToRaw(readChar(fname, file.info(fname)$size)))
  Results_final[[counter]] <- Results
 }

