#https://www.r-bloggers.com/identify-describe-plot-and-remove-the-outliers-from-the-dataset/
outlierKD <- function(dt, var) {
  var_name <- eval(substitute(var),eval(dt))
  tot <- sum(!is.na(var_name))
  na1 <- sum(is.na(var_name))
  m1 <- mean(var_name, na.rm = T)
  par(mfrow=c(2, 2), oma=c(0,0,3,0))
  boxplot(var_name, main="With outliers")
  hist(var_name, main="With outliers", xlab=NA, ylab=NA)
  outlier <- boxplot.stats(var_name)$out
  mo <- mean(outlier)
  var_name <- ifelse(var_name %in% outlier, NA, var_name)
  boxplot(var_name, main="Without outliers")
  hist(var_name, main="Without outliers", xlab=NA, ylab=NA)
  title("Outlier Check", outer=TRUE)
  na2 <- sum(is.na(var_name))
  cat("Outliers identified:", na2 - na1, "\n")
  cat("Propotion (%) of outliers:", round((na2 - na1) / tot*100, 1), "\n")
  cat("Mean of the outliers:", round(mo, 2), "\n")
  m2 <- mean(var_name, na.rm = T)
  cat("Mean without removing outliers:", round(m1, 2), "\n")
  cat("Mean if we remove outliers:", round(m2, 2), "\n")
  response <- readline(prompt="Do you want to remove outliers and to replace with NA? [yes/no]: ")
  if(response == "y" | response == "yes"){
    dt[as.character(substitute(var))] <- invisible(var_name)
    assign(as.character(as.list(match.call())$dt), dt, envir = .GlobalEnv)
    cat("Outliers successfully removed", "\n")
    return(invisible(dt))
  } else{
    cat("Nothing changed", "\n")
    return(invisible(var_name))
  }
}

## Plotting data -----
# http://stackoverflow.com/questions/10151123/how-to-specify-columns-in-facet-grid-or-how-to-change-labels-in-facet-wrap

library("ggplot2")
library("reshape2")

#example: 
df <- data.frame(x=1:10,
                 a=runif(10),
                 b=runif(10),
                 c=runif(10))

#melt your data
df_melt <- reshape2::melt(df,"x")

#scatterplot per group
ggplot(df_melt,aes(x,value)) +
  geom_point() +
  facet_grid(.~variable)


# example 2
df_melt2 <-reshape2::melt(Training_data_regression, "Position_change") # %>% dplyr::filter(variable %in% c("assets", "bps"))

ggplot(df_melt2,aes(x = value, y = Position_change)) +
  geom_point() +
  facet_wrap(~ variable, scales = "free", ncol = 5)

# Outliers Detection
library(HighDimOut)
res.ABOD <- Func.ABOD(data=TestData[,1:2], basic=FALSE, perc=0.2)
data.temp <- TestData[,1:2]
data.temp$Ind <- NA
data.temp[order(res.ABOD, decreasing = FALSE)[1:10],"Ind"] <- "Outlier"
data.temp[is.na(data.temp$Ind),"Ind"] <- "Inlier"
data.temp$Ind <- factor(data.temp$Ind)
ggplot(data = data.temp) + geom_point(aes(x = x, y = y, color=Ind, shape=Ind))


# Outliers

library(HighDimOut)
library(plyr)
data.scale1 <- t(aaply(.data = as.matrix(GoldenStatesWarriors[,-1]), .margins = 2, .fun = function(x) (x-mean(x, na.rm = T))/sd(x, na.rm = T)))
summary(data.scale1)
data.scale2 <- scale(GoldenStatesWarriors[, -1], center = TRUE, scale = TRUE )
summary(data.scale2)


# ROC analysis
# Method 2 -----
library(pROC)
#apply roc function
analysis <- roc(response=Training_data_classification$Buy_Sell, predictor=m_Logit$fitted.values)
e <- cbind(analysis$thresholds, analysis$sensitivities+analysis$specificities)
opt_t <- subset(e,e[,2]==max(e[,2]))[,1]

#Plot ROC Curve
plot(1-analysis$specificities,analysis$sensitivities,type="l",
     ylab="Sensitiviy",xlab="1-Specificity",col="black",lwd=2,
     main = "ROC Curve for Logit GLM")
abline(a=0,b=1)
abline(v = opt_t) #add optimal t to ROC curve
opt_t #print t

# Model 2
analysis <- roc(response=Training_data_classification$Buy_Sell, predictor=m_Logit_Bayesian$fitted.values)
e <- cbind(analysis$thresholds,1 * analysis$sensitivities+analysis$specificities)
opt_t <- subset(e,e[,2]==max(e[,2]))[,1]

#Plot ROC Curve
plot(1-analysis$specificities,analysis$sensitivities,type="l",
     ylab="Sensitiviy",xlab="1-Specificity",col="black",lwd=2,
     main = "ROC Curve for Baysian Logit GLM")
abline(a=0,b=1)
abline(v = opt_t) #add optimal t to ROC curve
opt_t

# Method 3 -----
library(ROCR)
# https://hopstat.wordpress.com/2014/12/19/a-small-introduction-to-the-rocr-package/
# Logit model
pred_logit <- prediction(predictions = m_Logit$fitted.values, labels = Training_data_classification$Buy_Sell)

slotNames(pred_logit)

roc.perf = performance(pred_logit, measure = "tpr", x.measure = "fpr")
plot(roc.perf)
abline(a=0, b= 1)
# The further away from the diagonal line, the better.
slotNames(roc.perf)


cost.perf = performance(pred_logit, "cost")
plot(cost.perf)
cost.perf_assymetric = performance(pred_logit, "cost", cost.fp = 2, cost.fn = 1)
plot(cost.perf_assymetric)
acc.perf = performance(pred_logit, measure = "acc")
plot(acc.perf)
MutualInformation.perf = performance(pred_logit, measure = "mi")
plot(MutualInformation.perf)
# Precision/Recall Curve
perf1 <- performance(pred_logit, "prec", "rec")
plot(perf1)

# Determining optimal cutoff point
pred_logit@cutoffs[[1]][which.min(roc.perf@y.values[[1]])]

# Minimizing cost associated with (False Positive, False Negative)
pred_logit@cutoffs[[1]][which.min(cost.perf@y.values[[1]])]
pred_logit@cutoffs[[1]][which.min(cost.perf_assymetric@y.values[[1]])]

# Maximizing accuracy
pred_logit@cutoffs[[1]][which.max(acc.perf@y.values[[1]])]

# the above command in More details
ind = which.max( slot(acc.perf, "y.values")[[1]] )
acc = slot(acc.perf, "y.values")[[1]][ind]
cutoff = slot(acc.perf, "x.values")[[1]][ind]
print(c(accuracy= acc, cutoff = cutoff))


####### TTR Indicator ------
View(df)
xts.df <- xts::xts(df$Value, order.by = df$Date)
tt_DEMA <- TTR::DEMA(xts.df, n = 4)
tt_ZLEMA <- TTR::ZLEMA(xts.df, n = 4)

# tt_fit <- forecast::auto.arima(xts.df)
plot(index(xts.df), xts.df,col="red")
lines(index(xts.df), tt_ZLEMA ,col="blue")
lines(index(xts.df), tt_DEMA,col="black")
