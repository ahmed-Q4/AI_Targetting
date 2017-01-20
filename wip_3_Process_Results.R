# The commands in this file process the results produced by "Step2_ModelFitting_4Funds_EC2.R" file
#
#
# Getting Stat results for the model that worked


Results_stats_EntryPosition  <- lapply(Results_finals$Entry, function(x) x$Results_stat) %>% dplyr::bind_rows()
Results_stats_ChangePosition <- lapply(Results_finals$Position_Change, function(x) x$Results_stat) %>% dplyr::bind_rows()
Results_stats_ExitPosition   <- lapply(Results_finals$Exit, function(x) x$Results_stat) %>% dplyr::bind_rows()
#write.csv(Results_stats, "Results_stat_1.csv")


Results_warning_EntryPosition  <- lapply(Results_finals$Entry, function(x) if(!is.null(x$RegFit_warning) && x$RegFit_warning == TRUE) data.frame(Fund_ID = x$Fund_ID, WarningMsg = x$Warning_Regression$message)) %>%
                                  dplyr::bind_rows()
Results_warning_ChangePosition <- lapply(Results_finals$Position_Change, function(x) if(!is.null(x$RegFit_warning) && x$RegFit_warning == TRUE) data.frame(Fund_ID = x$Fund_ID, WarningMsg = x$Warning_Regression$message)) %>%
                                  dplyr::bind_rows()
Results_warning_ExitPosition   <- lapply(Results_finals$Exit, function(x) if(!is.null(x$RegFit_warning) && x$RegFit_warning == TRUE) data.frame(Fund_ID = x$Fund_ID, WarningMsg = x$Warning_Regression$message)) %>%
                                  dplyr::bind_rows()
#write.csv(Results_warning, "Results_warn_1.csv")


Results_error_EntryPosition  <- lapply(Results_finals$Entry, function(x) if(!is.null(x$RegFit_error) && x$RegFit_error == TRUE) data.frame(Fund_ID = x$Fund_ID, ErrorMsg = x$Error_Regression$message)) %>%
                               dplyr::bind_rows()
Results_error_ChangePosition <- lapply(Results_finals$Position_Change, function(x) if(!is.null(x$RegFit_error) && x$RegFit_error == TRUE) data.frame(Fund_ID = x$Fund_ID, ErrorMsg = x$Error_Regression$message)) %>%
                                dplyr::bind_rows()
Results_error_ExitPosition <- lapply(Results_finals$Exit, function(x) if(!is.null(x$RegFit_error) && x$RegFit_error == TRUE) data.frame(Fund_ID = x$Fund_ID, ErrorMsg = x$Error_Regression$message)) %>%
                                dplyr::bind_rows()
#write.csv(Results_error, "Results_error_1.csv")

Results_ProbThreshold_infinite_EntryPosition   <- sapply(Results_finals$Entry, function(x) if(!is.null(x$Prob_Threshold) && is.infinite(x$Prob_Threshold) == TRUE) x$Fund_ID) %>% unlist()
Results_ProbThreshold_infinite_ChangePosition <- sapply(Results_finals$Position_Change, function(x) if(!is.null(x$Prob_Threshold) && is.infinite(x$Prob_Threshold) == TRUE) x$Fund_ID) %>% unlist()
Results_ProbThreshold_infinite_ExitPosition    <- sapply(Results_finals$Exit, function(x) if(!is.null(x$Prob_Threshold) && is.infinite(x$Prob_Threshold) == TRUE) x$Fund_ID) %>% unlist()
#write.csv(Results_ProbThreshold_infinite, "Results_Class_Threshold_infinite_1.csv")

Results_Insufficient_Data_less20_EntryPosition  <- lapply(Results_finals$Entry, function(x) if(!is.null(x$Insufficient_data) && x$Insufficient_data == TRUE) x$Fund_ID) %>% unlist()
Results_Insufficient_Data_less20_ChangePosition <- lapply(Results_finals$Position_Change, function(x) if(!is.null(x$Insufficient_data) && x$Insufficient_data == TRUE) x$Fund_ID) %>% unlist()
Results_Insufficient_Data_less20_ExitPosition   <- lapply(Results_finals$Exit, function(x) if(!is.null(x$Insufficient_data) && x$Insufficient_data == TRUE) x$Fund_ID) %>% unlist()
#write.csv(Results_Insufficient_Data_less20, "Results_insuficient_Hld_pts_1.csv")

### Graphing Results
Results_stats_numeric_EntryPosition <- dplyr::select(Results_stats_EntryPosition, -Prob_pred_Buy_given_Buy_Lasso, -Prob_pred_Buy_given_Sell_Lasso,
                                                     -Prob_pred_Sell_given_Buy_Lasso, -Prob_pred_Sell_given_Sell_Lasso) %>%
                                       dplyr::mutate(Prob_No_Entry_given_pred_No_Entry = sapply(strsplit(Prob_Buy_given_pred_Buy_Lasso,  split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                     Prob_No_Entry_given_pred_Entry    = sapply(strsplit(Prob_Buy_given_pred_Sell_Lasso, split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                     Prob_Entry_given_pred_No_Entry    = sapply(strsplit(Prob_Sell_given_pred_Buy_Lasso, split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                     Prob_Entry_given_pred_Entry       = sapply(strsplit(Prob_Sell_given_pred_Sell_Lasso,split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                     Pred_Accuracy                     = sapply(strsplit(Pred_Accuracy_GLMnet_Lasso,     split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100)) %>%
                                       dplyr::select(-dplyr::contains("_Lasso"))


Results_stats_numeric_ChangePosition <- dplyr::select(Results_stats_ChangePosition, -Prob_pred_Buy_given_Buy_Lasso, -Prob_pred_Buy_given_Sell_Lasso,
                                                      -Prob_pred_Sell_given_Buy_Lasso, -Prob_pred_Sell_given_Sell_Lasso) %>%
                                        dplyr::mutate(Prob_Buy_given_pred_Buy   = sapply(strsplit(Prob_Buy_given_pred_Buy_Lasso,  split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                      Prob_Buy_given_pred_Sell  = sapply(strsplit(Prob_Buy_given_pred_Sell_Lasso, split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                      Prob_Sell_given_pred_Buy  = sapply(strsplit(Prob_Sell_given_pred_Buy_Lasso, split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                      Prob_Sell_given_pred_Sell = sapply(strsplit(Prob_Sell_given_pred_Sell_Lasso,split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                      Pred_Accuracy             = sapply(strsplit(Pred_Accuracy_GLMnet_Lasso,     split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100)) %>%
                                         dplyr::select(-dplyr::contains("_Lasso"))



Results_stats_numeric_ExitPosition <- dplyr::select(Results_stats_ExitPosition, -Prob_pred_Buy_given_Buy_Lasso, -Prob_pred_Buy_given_Sell_Lasso,
                                                    -Prob_pred_Sell_given_Buy_Lasso, -Prob_pred_Sell_given_Sell_Lasso) %>%
                                      dplyr::mutate(Prob_No_Exit_given_pred_No_Exit = sapply(strsplit(Prob_Buy_given_pred_Buy_Lasso,  split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                    Prob_No_Exit_given_pred_Exit    = sapply(strsplit(Prob_Buy_given_pred_Sell_Lasso, split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                    Prob_Exit_given_pred_No_Exit    = sapply(strsplit(Prob_Sell_given_pred_Buy_Lasso, split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                    Prob_Exit_given_pred_Exit       = sapply(strsplit(Prob_Sell_given_pred_Sell_Lasso,split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100),
                                                    Pred_Accuracy                   = sapply(strsplit(Pred_Accuracy_GLMnet_Lasso,     split = " %", fixed = TRUE), function(x) as.numeric(x[1])/100)) %>%
                                      dplyr::select(-dplyr::contains("_Lasso"))

# Replacing NaN with NA
# http://r.789695.n4.nabble.com/Dealing-with-NaN-s-in-data-frames-td865965.html
Results_stats_numeric_EntryPosition[is.na(Results_stats_numeric_EntryPosition)]   <- NA
Results_stats_numeric_ChangePosition[is.na(Results_stats_numeric_ChangePosition)] <- NA
Results_stats_numeric_ExitPosition[is.na(Results_stats_numeric_ExitPosition)]     <- NA
# is.na(Results_stats_numeric) <- is.na(Results_stats_numeric) 
# Results_stats_numeric[is.na(Results_stats_numeric)] <- "--"


library(ggplot2)
library(GGally)
# http://ggobi.github.io/ggally
plotList_EntryPosition  <- vector(mode = "list", length = 4)
plotList_ChangePosition <- vector(mode = "list", length = 4)
plotList_ExitPosition   <- vector(mode = "list", length = 4)

# Entry Position
plotList_EntryPosition[[1]] <- ggplot(Results_stats_numeric_EntryPosition, aes(x = Prob_No_Entry_given_pred_No_Entry))   + geom_histogram(binwidth = 0.05)
plotList_EntryPosition[[2]] <- ggplot(Results_stats_numeric_EntryPosition, aes(x = Prob_No_Entry_given_pred_Entry))      + geom_histogram(binwidth = 0.05)
plotList_EntryPosition[[3]] <- ggplot(Results_stats_numeric_EntryPosition, aes(x = Prob_Entry_given_pred_No_Entry))      + geom_histogram(binwidth = 0.05)
plotList_EntryPosition[[4]] <- ggplot(Results_stats_numeric_EntryPosition, aes(x = Prob_Entry_given_pred_Entry))         + geom_histogram(binwidth = 0.05)

# Change Position
plotList_ChangePosition[[1]] <- ggplot(Results_stats_numeric_ChangePosition, aes(x = Prob_Buy_given_pred_Buy))   + geom_histogram(binwidth = 0.05)
plotList_ChangePosition[[2]] <- ggplot(Results_stats_numeric_ChangePosition, aes(x = Prob_Buy_given_pred_Sell))  + geom_histogram(binwidth = 0.05)
plotList_ChangePosition[[3]] <- ggplot(Results_stats_numeric_ChangePosition, aes(x = Prob_Sell_given_pred_Buy))  + geom_histogram(binwidth = 0.05)
plotList_ChangePosition[[4]] <- ggplot(Results_stats_numeric_ChangePosition, aes(x = Prob_Sell_given_pred_Sell)) + geom_histogram(binwidth = 0.05)


# Exit Position
plotList_ExitPosition[[1]] <- ggplot(Results_stats_numeric_ExitPosition, aes(x = Prob_No_Exit_given_pred_No_Exit))   + geom_histogram(binwidth = 0.05)
plotList_ExitPosition[[2]] <- ggplot(Results_stats_numeric_ExitPosition, aes(x = Prob_No_Exit_given_pred_Exit))      + geom_histogram(binwidth = 0.05)
plotList_ExitPosition[[3]] <- ggplot(Results_stats_numeric_ExitPosition, aes(x = Prob_Exit_given_pred_No_Exit))      + geom_histogram(binwidth = 0.05)
plotList_ExitPosition[[4]] <- ggplot(Results_stats_numeric_ExitPosition, aes(x = Prob_Exit_given_pred_Exit))         + geom_histogram(binwidth = 0.05)



# Plotting grid!
pm_EntryPosition <- ggmatrix(plots = plotList_EntryPosition,
                             nrow = 2, ncol = 2,
                             xAxisLabels = c("Predicted No Entry", "Predicted Entry"),
                             yAxisLabels = c("Enter Position", "Does Not Enter Position"),
                             title = "Histogram (Obs|Prediction) - Confusion Matrix"
                             ) + ggthemes::theme_solarized(light = TRUE)

pm_ChangePosition <- ggmatrix(plots = plotList_ChangePosition,
                             nrow = 2, ncol = 2,
                             xAxisLabels = c("Predicted Buy", "Predicted Sell"),
                             yAxisLabels = c("Buy", "Sell"),
                             title = "Histogram (Obs|Prediction) - Confusion Matrix"
                      ) + ggthemes::theme_solarized(light = TRUE)

pm_ExitPosition <- ggmatrix(plots = plotList_ExitPosition,
                             nrow = 2, ncol = 2,
                             xAxisLabels = c("Predicted No Exit", "Predicted Exit"),
                             yAxisLabels = c("Exit Position", "Does Not Exit Position"),
                             title = "Histogram (Obs|Prediction) - Confusion Matrix"
                              ) + ggthemes::theme_solarized(light = TRUE)

# print(pm)
# Saving results to excel
library(openxlsx)
# https://cran.r-project.org/web/packages/openxlsx/vignettes/Introduction.pdf
# Creating a workbook ----
wb_Results <- createWorkbook()


# Creating sheets ----
# Entry Position Results sheet
sheet_stat_EntryPosition <- addWorksheet(wb_Results, sheetName = "Predictive Statistics - Entry")
sheet_plot_EntryPosition <- addWorksheet(wb_Results, sheetName = "Histograms - Entry")
sheet_warn_EntryPosition <- addWorksheet(wb_Results, sheetName = "Warnings - Entry")
sheet_err_EntryPosition  <- addWorksheet(wb_Results, sheetName = "Errors - Entry")
sheet_thresh_inf_EntryPosition <- addWorksheet(wb_Results, sheetName = "Inf_ClassThreshold - Entry")
sheet_insuff_pts_EntryPosition <- addWorksheet(wb_Results, sheetName = "LessThan20_SamplePts - Entry")

# Change Position Results sheet
sheet_stat_ChangePosition <- addWorksheet(wb_Results, sheetName = "Predictive Statistics - Change")
sheet_plot_ChangePosition <- addWorksheet(wb_Results, sheetName = "Histograms - Change")
sheet_warn_ChangePosition <- addWorksheet(wb_Results, sheetName = "Warnings - Change")
sheet_err_ChangePosition  <- addWorksheet(wb_Results, sheetName = "Errors - Change")
sheet_thresh_inf_ChangePosition <- addWorksheet(wb_Results, sheetName = "Inf_ClassThreshold - Change")
sheet_insuff_pts_ChangePosition <- addWorksheet(wb_Results, sheetName = "LessThan20_SamplePts - Change")

# Exit Position Results sheet
sheet_stat_ExitPosition <- addWorksheet(wb_Results, sheetName = "Predictive Statistics - Exit")
sheet_plot_ExitPosition <- addWorksheet(wb_Results, sheetName = "Histograms - Exit")
sheet_warn_ExitPosition <- addWorksheet(wb_Results, sheetName = "Warnings - Exit")
sheet_err_ExitPosition  <- addWorksheet(wb_Results, sheetName = "Errors - Exit")
sheet_thresh_inf_ExitPosition <- addWorksheet(wb_Results, sheetName = "Inf_ClassThreshold - Exit")
sheet_insuff_pts_ExitPosition <- addWorksheet(wb_Results, sheetName = "LessThan20_SamplePts - Exit")


# Saving results to sheets ----
# Entry Position
print(pm_EntryPosition)
insertPlot(wb = wb_Results, sheet = sheet_plot_EntryPosition, startRow = 4,startCol = 3, width = 6, height = 5)
writeData(wb = wb_Results, x = as.data.frame(Results_stats_numeric_EntryPosition), sheet = sheet_stat_EntryPosition, rowNames=FALSE)
writeData(wb = wb_Results, x = as.data.frame(Results_warning_EntryPosition), sheet = sheet_warn_EntryPosition, rowNames=FALSE)
writeData(wb = wb_Results, x = as.data.frame(Results_error_EntryPosition), sheet = sheet_err_EntryPosition, rowNames=FALSE)
writeData(wb = wb_Results, x = data.frame(FundID = Results_ProbThreshold_infinite_EntryPosition), sheet = sheet_thresh_inf_EntryPosition, rowNames=FALSE)
writeData(wb = wb_Results, x = data.frame(FundID = Results_Insufficient_Data_less20_EntryPosition), sheet = sheet_insuff_pts_EntryPosition, rowNames=FALSE)


# Change Position
print(pm_ChangePosition)
insertPlot(wb = wb_Results, sheet = sheet_plot_ChangePosition, startRow = 4,startCol = 3, width = 6, height = 5)
# writeData(wb = wb_Results, x = as.data.frame(Results_stats), sheet = sheet_stat, rowNames=FALSE)
writeData(wb = wb_Results, x = as.data.frame(Results_stats_numeric_ChangePosition), sheet = sheet_stat_ChangePosition, rowNames=FALSE)
writeData(wb = wb_Results, x = as.data.frame(Results_warning_ChangePosition), sheet = sheet_warn_ChangePosition, rowNames=FALSE)
writeData(wb = wb_Results, x = as.data.frame(Results_error_ChangePosition), sheet = sheet_err_ChangePosition, rowNames=FALSE)
writeData(wb = wb_Results, x = data.frame(FundID = Results_ProbThreshold_infinite_ChangePosition), sheet = sheet_thresh_inf_ChangePosition, rowNames=FALSE)
writeData(wb = wb_Results, x = data.frame(FundID = Results_Insufficient_Data_less20_ChangePosition), sheet = sheet_insuff_pts_ChangePosition, rowNames=FALSE)

# Exit
print(pm_ExitPosition)
insertPlot(wb = wb_Results, sheet = sheet_plot_ExitPosition, startRow = 4,startCol = 3, width = 6, height = 5)
writeData(wb = wb_Results, x = as.data.frame(Results_stats_numeric_ExitPosition), sheet = sheet_stat_ExitPosition, rowNames=FALSE)
writeData(wb = wb_Results, x = as.data.frame(Results_warning_ExitPosition), sheet = sheet_warn_ExitPosition, rowNames=FALSE)
writeData(wb = wb_Results, x = as.data.frame(Results_error_ExitPosition), sheet = sheet_err_ExitPosition, rowNames=FALSE)
writeData(wb = wb_Results, x = data.frame(FundID = Results_ProbThreshold_infinite_ExitPosition), sheet = sheet_thresh_inf_ExitPosition, rowNames=FALSE)
writeData(wb = wb_Results, x = data.frame(FundID = Results_Insufficient_Data_less20_ExitPosition), sheet = sheet_insuff_pts_ExitPosition, rowNames=FALSE)


# Saving workbook & cleaning up -----
saveWorkbook(wb_Results, paste0("Results_No_Sector_No_CAP_features_", Results_finals$Fitting_Algorithm,"_.xlsx"),
             overwrite = TRUE)
# cleaning up
rm(wb_Results,
   Results_stats_EntryPosition, Results_stats_ChangePosition, Results_stats_ExitPosition,
   Results_stats_numeric_EntryPosition, Results_stats_numeric_ChangePosition, Results_stats_numeric_ExitPosition,
   Results_warning_EntryPosition, Results_warning_ChangePosition, Results_warning_ExitPosition,
   Results_error_EntryPosition, Results_error_ChangePosition, Results_error_ExitPosition,
   Results_ProbThreshold_infinite_EntryPosition, Results_ProbThreshold_infinite_ChangePosition, Results_ProbThreshold_infinite_ExitPosition,
   Results_Insufficient_Data_less20_EntryPosition, Results_Insufficient_Data_less20_ChangePosition, Results_Insufficient_Data_less20_ExitPosition,
   sheet_stat_EntryPosition, sheet_stat_ChangePosition, sheet_stat_ExitPosition,
   sheet_plot_EntryPosition, sheet_plot_ChangePosition, sheet_plot_ExitPosition,
   sheet_warn_EntryPosition, sheet_warn_ChangePosition, sheet_warn_ExitPosition,
   sheet_err_EntryPosition, sheet_err_ChangePosition, sheet_err_ExitPosition,
   sheet_thresh_inf_EntryPosition, sheet_thresh_inf_ChangePosition, sheet_thresh_inf_ExitPosition,
   sheet_insuff_pts_EntryPosition, sheet_insuff_pts_ChangePosition, sheet_insuff_pts_ExitPosition,
   plotList_EntryPosition, plotList_ChangePosition, plotList_ExitPosition,
   pm_EntryPosition, pm_ChangePosition, pm_ExitPosition)