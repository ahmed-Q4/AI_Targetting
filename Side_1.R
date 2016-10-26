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
