library(randomForest)
library(caret)
library(aws.s3)

FundRatings <- read_csv("~/R_workspaces/AI_Targetting/FundRatings.csv",
                        col_types = cols(AverageMarketValueRating = col_double(),
                                         NumberPositions = col_double(), OIPRating = col_double(),
                                         OIPRatingOffset = col_double(), StyleRating = col_double(),
                                         TotalMarketValueRating = col_double(),TurnoverRating = col_double()
                                         )
                        ) %>% dplyr::select(Fund_ID = FactSetFundId, FundRating = OIPRating)

fill_missing_if_any <- function(data.df, value.df){
  count <- matrix(0, nrow = NROW(data.df), ncol = 1)
  for(j in 1:NCOL(data.df)) {
    indx <- which(is.na(data.df[,j]))
    if(length(indx) > 0)
      count[indx, 1] <- count[indx, 1] + 1
      data.df[indx, j] <- value.df[j, 2]
  }
  res <- list(data = data.df, Count = count)
  return(res)
}

my_pred <- function(ticker, model_path, fund, Models_on_S3 = FALSE, bucket = NULL, Indicator_Data) {
  if(Models_on_S3 == TRUE) {
    fname <- fund
    mm <- unserialize(get_object(fname, bucket = bucket))
  } else {
    fname <- paste0(model_path, fund, ".txt")
    mm <- unserialize(charToRaw(readChar(fname, file.info(fname)$size)))
  }
  varNames <- names(mm$trainingData)[1:(NCOL(mm$trainingData) -1)]
  mm_avg <- data.frame(colMeans(mm$trainingData[, 1:(NCOL(mm$trainingData) -1)], na.rm = TRUE)) %>%
            tibble::rownames_to_column(var = "VarNames")

  data4model0 <- Indicator_Data %>% dplyr::filter(Symbol == ticker) %>% dplyr::select_(.dots = varNames)
  # Filling missing data if there is any.
  data4model <- fill_missing_if_any(data.df = data4model0, value.df = mm_avg)
  newX <- data4model$data
  # if(any(is.na(newX))) {
  #   missingValue <- TRUE
  #   numMissingValue <- length(which(is.na(newX)))
  #   # Filling in missing values by the average of the variable in the training set of the model
  #   avg <- colMeans(mm$trainingData[, 1:(NCOL(mm$trainingData) -1)], na.rm = TRUE)
  #   newX[, which(is.na(newX))] <- avg[which(is.na(newX))]
  # } else {
  #   missingValue <- FALSE
  #   numMissingValue <- 0
  # }
  pred <- predict(mm$finalModel, newdata = newX ,predict.all=TRUE)
  votes <- apply(pred$individual,1,function(x){table(x)})
  ntrees <- mm$finalModel$ntree
  if(is.list(votes)){
    out<-c()
    for (i in 1:2){
      out<-cbind(out,sapply(votes,function(x) x[i]))
    }
    out[is.na(out)] <- 0
    rownames(out) <- NULL
    colnames(out) <- c("FALSE", "TRUE")
    VotesPerc <- t(out)/ntrees
  } else VotesPerc <- votes/ntrees
  
  fund_tmp <- strsplit(fund, split = "/", fixed = TRUE) %>% unlist()
  fund_id <- gsub(x = fund_tmp[2], pattern = ".txt", replacement = "", fixed = TRUE)
  modeling <- strsplit(fund_tmp[1], split = "_", fixed = TRUE)[[1]][2]
  res <- data.frame(model = fund,
                    modeling = modeling,
                    list_votes_debug = is.list(votes),
                    Fund_ID = fund_id,
                    N_Training = NROW(mm$trainingData),
                    MissingValues = data4model$Count,
                    NumVar = NROW(mm_avg),
                    Symbol = ticker,
                    ClassPred = pred$aggregate,
                    VotesPerc_false = t(VotesPerc)[, 1],
                    VotesPerc_true =  t(VotesPerc)[, 2],
                    stringsAsFactors = FALSE) %>% 
          dplyr::mutate(VotePerc = ifelse(ClassPred %in% c("TRUE", "Sell"),VotesPerc_true, VotesPerc_false )) %>%
          dplyr::select(-VotesPerc_false, -VotesPerc_true)
  return(res)
}

var_imp <- function(model_path, fund, Models_on_S3 = FALSE, bucket = NULL) {
  if(Models_on_S3 == TRUE) {
    fname <- fund
    mm <- unserialize(get_object(fname, bucket = bucket))
  } else {
    fname <- paste0(model_path, fund, ".txt")
    mm <- unserialize(charToRaw(readChar(fname, file.info(fname)$size)))
  }
  browser()
  res_tmp <- caret::varImp(mm)
  # Getting ID of fund to include in results
  fund_tmp <- strsplit(fund, split = "/", fixed = TRUE) %>% unlist()
  fund_id <- gsub(x = fund_tmp[2], pattern = ".txt", replacement = "", fixed = TRUE)
  
  
  res <- res_tmp$importance %>% tibble::rownames_to_column(var = "VariableName") %>% dplyr::mutate(FundId = fund_id)
  names(res)[2] <- "Importance"
  return(res)
  
}


S3bucket <- get_bucket(bucket = 'q4quant-ai-targeting-models')
files_list <- NULL
files_list_tmp <- data.frame(FileName = sapply(S3bucket, FUN = function(x){x$Key}), stringsAsFactors = FALSE)
while((NROW(files_list_tmp) %% 1000  == 0) && NROW(files_list_tmp) == 1000) {
  files_list <- rbind(files_list, files_list_tmp)
  S3bucket_tmp <- get_bucket(bucket = 'q4quant-ai-targeting-models', marker = files_list$FileName[NROW(files_list)])
  files_list_tmp <- data.frame(FileName = sapply(S3bucket_tmp, FUN = function(x){x$Key}), stringsAsFactors = FALSE)
}
files_list <- rbind(files_list, files_list_tmp)
rm(files_list_tmp)

model_entry <- files_list %>% dplyr::filter(grepl(pattern = "1_EntryPosition/", x = FileName), FileName != "1_EntryPosition/")
model_change <- files_list %>% dplyr::filter(grepl(pattern = "2_ChangePosition/", x = FileName), FileName != "2_ChangePosition/")
model_exit <- files_list %>% dplyr::filter(grepl(pattern = "3_ExitPosition/", x = FileName), FileName != "3_ExitPosition/")


#modelPath <- list(EntryModelPath, ChangeModelPath, ExitModelPath)
models <- list(model_entry, model_change, model_exit)


# Determining Variable Importance for the Fitted Models
res_varImp <- vector(mode = "list", length = 3)
for(j in 1:3){
  res_varImp[[j]] <- lapply(X = models[[j]]$FileName, FUN = var_imp, model_path = NULL, Models_on_S3 = TRUE, 
                     bucket = "q4quant-ai-targeting-models") %>% dplyr::bind_rows()
}


res_varImp[[1]] <- res_varImp[[1]] %>% dplyr::arrange(FundId, desc(Importance))
res_varImp[[2]] <- res_varImp[[2]] %>% dplyr::arrange(FundId, desc(Importance))
res_varImp[[3]] <- res_varImp[[3]] %>% dplyr::arrange(FundId, desc(Importance))


# res_varImp_final <- vector(mode = "list", length = 3)
# for(j in 1:3){
#   res_varImp_final[[j]] <- dplyr::arrange(res_varImp[[1]][[1]], desc(Importance))
#   for(k in 1:length(res_varImp[[1]])){
#     if(k %%  10 == 0) print(paste(j, k, sep = "-"))
#     res_varImp_final[[j]]  <- qpcR:::cbind.na(res_varImp_final[[j]],
#                                               dplyr::arrange(res_varImp[[j]][[k]], desc(Importance)))
#   }
# }

library(openxlsx)
# https://cran.r-project.org/web/packages/openxlsx/vignettes/Introduction.pdf
# Creating a workbook ----
wb_Results <- createWorkbook()
sheet_entry <- addWorksheet(wb_Results, sheetName = "Entry")
sheet_change <- addWorksheet(wb_Results, sheetName = "Change")
sheet_exit <- addWorksheet(wb_Results, sheetName = "Exit")

writeData(wb = wb_Results, x = as.data.frame(res_varImp[[1]]), sheet = sheet_entry, rowNames=FALSE)
writeData(wb = wb_Results, x = as.data.frame(res_varImp[[2]]), sheet = sheet_change, rowNames=FALSE)
writeData(wb = wb_Results, x = as.data.frame(res_varImp[[3]]), sheet = sheet_exit, rowNames=FALSE)

saveWorkbook(wb_Results, file = "Funds_VariableRanking.xlsx", overwrite = TRUE)
rm(sheet_entry, sheet_change, sheet_exit, wb_Results)

#### Getting Funds prediction for symbolList
res <- vector(mode = "list", length = 3)
for(j in 1:3){
  res[[i]] <- lapply(X = models[[j]]$FileName, FUN = my_pred, ticker = SymbolList, model_path = NULL, Models_on_S3 = TRUE, 
                    bucket = "q4quant-ai-targeting-models", Indicator_Data = Mrk_Data) %>% dplyr::bind_rows()
}

fname <- "./ModelResults/04B8FN-E.txt"
mm <- unserialize(charToRaw(readChar(fname, file.info(fname)$size)))


res[[1]] <- res_tmp %>% dplyr::mutate(Fund_ID = gsub(pattern = "-E-1", replacement = "-E", x = Fund_ID)) %>%
            dplyr::left_join(supportiveData$GlobalModelStat[[1]], by = c("Fund_ID" = "Fund")) %>%
            dplyr::left_join(supportiveData$FundName, by = c("Fund_ID" = "Fund")) %>%
            dplyr::left_join(FundRatings, by = c("Fund_ID")) %>%
            dplyr::select(-Pred_Accuracy) %>%
            dplyr::mutate(Score = TP * VotePerc,
                          Score_with_fund = round(Score * FundRating, digits = 0),
                          p_2.5  = quantile(Score_with_fund, probs = 0.025, na.rm = TRUE),
                          #Score_shift = p_2.5
                          Score_shift = 0) %>%
                          dplyr::rowwise() %>%
                          dplyr::mutate(Score_tmp = max(Score_with_fund - Score_shift, 0, na.rm = TRUE)) %>%
                          dplyr::ungroup() %>%
                          dplyr::mutate(p_97.5 = quantile(Score_tmp, probs = 0.975, na.rm = TRUE)) %>%
                          dplyr::rowwise() %>%
                          dplyr::mutate(Score_tmp2 = min(Score_tmp, p_97.5, na.rm = TRUE)) %>%
                          dplyr::ungroup() %>%
                          dplyr::mutate(Score_final = round(100 * Score_tmp2/max(Score_tmp2, na.rm = TRUE), digits = 0)) %>%
            # dplyr::group_by(Symbol, ClassPred) %>%
            # dplyr::mutate(ScalingMin = min(Score_with_fund, na.rm = TRUE),
            #               ScalingFactor = max(Score_with_fund - ScalingMin, na.rm = TRUE),
            #               Scaled_Score_with_fund = round(100 * (Score_with_fund - ScalingMin)/ScalingFactor, digits = 0)) %>%
            # dplyr::ungroup() %>%
            dplyr::arrange(Symbol, desc(ClassPred),desc(Score_final))

res[[2]] <- res_tmp2 %>% dplyr::mutate(Fund_ID = gsub(pattern = "-E-1", replacement = "-E", x = Fund_ID),
                                       Fund_ID = gsub(pattern = "-E-1", replacement = "-E", x = Fund_ID)) %>%
            dplyr::left_join(supportiveData$GlobalModelStat[[2]], by = c("Fund_ID" = "Fund")) %>%
            dplyr::left_join(supportiveData$FundName, by = c("Fund_ID" = "Fund")) %>%
            dplyr::left_join(FundRatings, by = c("Fund_ID")) %>%
            dplyr::select(-Pred_Accuracy) %>%
            dplyr::mutate(Score = ifelse(ClassPred == "Sell", TP * VotePerc, TN * VotePerc),
                          Score_with_fund = round(Score * FundRating, digits = 0),
                          p_2.5  = quantile(Score_with_fund, probs = 0.025, na.rm = TRUE),
                          #Score_shift = p_2.5
                          Score_shift = 0) %>%
            dplyr::rowwise() %>%
            dplyr::mutate(Score_tmp = max(Score_with_fund - Score_shift, 0, na.rm = TRUE)) %>%
            dplyr::ungroup() %>%
            dplyr::mutate(p_97.5 = quantile(Score_tmp, probs = 0.975, na.rm = TRUE)) %>%
            dplyr::rowwise() %>%
            dplyr::mutate(Score_tmp2 = min(Score_tmp, p_97.5, na.rm = TRUE)) %>%
            dplyr::ungroup() %>%
            dplyr::mutate(Score_final = round(100 * Score_tmp2/max(Score_tmp2, na.rm = TRUE), digits = 0)) %>%
            # dplyr::group_by(Symbol, ClassPred) %>%
            # dplyr::mutate(ScalingMin = min(Score_with_fund, na.rm = TRUE),
            #               ScalingFactor = max(Score_with_fund - ScalingMin, na.rm = TRUE),
            #               Scaled_Score_with_fund = round(100 * (Score_with_fund - ScalingMin)/ScalingFactor, digits = 0)) %>%
            # dplyr::ungroup() %>%
            dplyr::arrange(Symbol, ClassPred, desc(Score_final))

res[[3]] <- res_tmp3 %>% dplyr::mutate(Fund_ID = gsub(pattern = "-E-1", replacement = "-E", x = Fund_ID),
                                       Fund_ID = gsub(pattern = "-E-1", replacement = "-E", x = Fund_ID)) %>%
            dplyr::left_join(supportiveData$GlobalModelStat[[3]], by = c("Fund_ID" = "Fund")) %>%
            dplyr::left_join(supportiveData$FundName, by = c("Fund_ID" = "Fund")) %>%
            dplyr::left_join(FundRatings, by = c("Fund_ID")) %>%
            dplyr::select(-Pred_Accuracy) %>%
            dplyr::mutate(Score = TP * VotePerc,
                          Score_with_fund = round(Score * FundRating, digits = 0),
                          p_2.5  = quantile(Score_with_fund, probs = 0.025, na.rm = TRUE),
                          #Score_shift = p_2.5
                          Score_shift = 0) %>%
            dplyr::rowwise() %>%
            dplyr::mutate(Score_tmp = max(Score_with_fund - Score_shift, 0, na.rm = TRUE)) %>%
            dplyr::ungroup() %>%
            dplyr::mutate(p_97.5 = quantile(Score_tmp, probs = 0.975, na.rm = TRUE)) %>%
            dplyr::rowwise() %>%
            dplyr::mutate(Score_tmp2 = min(Score_tmp, p_97.5, na.rm = TRUE)) %>%
            dplyr::ungroup() %>%
            dplyr::mutate(Score_final = round(100 * Score_tmp2/max(Score_tmp2, na.rm = TRUE), digits = 0)) %>%
                      # dplyr::group_by(Symbol, ClassPred) %>%
                      # dplyr::mutate(ScalingMin = min(Score_with_fund, na.rm = TRUE),
                      #               ScalingFactor = max(Score_with_fund - ScalingMin, na.rm = TRUE),
                      #               Scaled_Score_with_fund = round(100 * (Score_with_fund - ScalingMin)/ScalingFactor, digits = 0)) %>%
                      # dplyr::ungroup() %>%
                      dplyr::arrange(Symbol, desc(ClassPred),desc(Score_final))
library(ggplot2)
p1 <- ggplot(res[[1]], aes(x = Score_final)) + geom_histogram(aes(y = (..count..)/sum(..count..)), binwidth = 5) +
      scale_y_continuous(labels = scales::percent) +
      labs(title="Histogram of Final Score for Entry Models", x ="Score final", y = "Frequency") +
      ggthemes::theme_solarized(light = TRUE)
p1


p2 <- ggplot(res[[2]], aes(x = Score_final)) + geom_histogram(aes(y = (..count..)/sum(..count..)), binwidth = 5) +
      scale_y_continuous(labels = scales::percent) +
      labs(title="Histogram of Final Score for Change Models", x ="Score final", y = "Frequency") +
      ggthemes::theme_solarized(light = TRUE)
p2

p3 <- ggplot(res[[3]], aes(x = Score_final)) + geom_histogram(aes(y = (..count..)/sum(..count..)), binwidth = 5) +
      scale_y_continuous(labels = scales::percent) +
      labs(title="Histogram of Final Score for Exit Models", x ="Score final", y = "Frequency") +
      ggthemes::theme_solarized(light = TRUE)
p3
      
library(openxlsx)
# https://cran.r-project.org/web/packages/openxlsx/vignettes/Introduction.pdf
# Creating a workbook ----
wb_Results <- createWorkbook()
sheet_entry <- addWorksheet(wb_Results, sheetName = "Entry")
sheet_change <- addWorksheet(wb_Results, sheetName = "Change")
sheet_exit <- addWorksheet(wb_Results, sheetName = "Exit")

sheet_entry_hist <- addWorksheet(wb_Results, sheetName = "Entry hist")
sheet_change_hist <- addWorksheet(wb_Results, sheetName = "Change hist")
sheet_exit_hist <- addWorksheet(wb_Results, sheetName = "Exit hist")

print(p1)
insertPlot(wb = wb_Results, sheet = sheet_entry_hist, startRow = 4,startCol = 3, width = 6, height = 5)
writeData(wb = wb_Results, x = as.data.frame(res[[1]]), sheet = sheet_entry, rowNames=FALSE)
print(p2)
insertPlot(wb = wb_Results, sheet = sheet_change_hist, startRow = 4,startCol = 3, width = 6, height = 5)
writeData(wb = wb_Results, x = as.data.frame(res[[2]]), sheet = sheet_change, rowNames=FALSE)
print(p3)
insertPlot(wb = wb_Results, sheet = sheet_exit_hist, startRow = 4,startCol = 3, width = 6, height = 5)
writeData(wb = wb_Results, x = as.data.frame(res[[3]]), sheet = sheet_exit, rowNames=FALSE)

saveWorkbook(wb_Results, file = "Fund_Recommendation_AI_Targeting_1_.xlsx", overwrite = TRUE)
rm(sheet_entry, sheet_entry_hist, sheet_change, sheet_change_hist, sheet_exit,sheet_exit_hist, wb_Results)

