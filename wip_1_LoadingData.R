
# library(data.table)
# mydata<-fread("./own_13f_holdings_hist_1.txt")


# Shark Position Processing

SharkPositions <- read.csv("./distinctSecurities.csv", stringsAsFactors=FALSE) %>% 
                  dplyr::filter(SharkGrouping == "True") %>% dplyr::mutate(Date = as.Date(Date, format = "%m/%d/%Y"),
                                                                           Year = format(Date, "%Y"))

Positions_dates <- sort(unique(SharkPositions$Date))


# Rudimentary analysis -----
Transaction_per_year <- dplyr::group_by(SharkPositions, Year) %>% dplyr::summarise(Count = n())
Symbol_per_year <- dplyr::group_by(SharkPositions, Year) %>% dplyr::summarise(Count = length(unique(Symbol)))
transaction_per_symbol <- dplyr::group_by(SharkPositions, Symbol) %>% dplyr::summarise(Count = n()) %>% dplyr::arrange(desc(Count))
transaction_per_symbol_per_year <- dplyr::group_by(SharkPositions, Symbol, Year) %>% dplyr::summarise(Count = n()) %>% dplyr::arrange(desc(Count))
good_symbols_list <- dplyr::filter(transaction_per_symbol_per_year, Count <= 4) %>% dplyr::distinct()
write.csv(transaction_per_symbol_per_year, "Numbers_of_rows_per_symbol_per_year.csv")

# Saving Shark Position to db ----
library(RMySQL)
con <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")
dbListTables(con)
dbWriteTable(con, "SharkPositions", SharkPositions)

SharkPositions_good <- data.set <- dplyr::semi_join(SharkPositions, good_symbols_list, by = "Symbol")
dbWriteTable(con, "SharkPositions_good", SharkPositions_good)

# Processing fundamental data and saving it to db -----.
data_path <- "./fundamentals_10_15_2016/"
files <- dir(path = data_path, pattern = "_Request")
for(f in files) {
  message(paste("Processing File:", f))
  data_field <- substring(f, 4) %>% gsub(pattern = "_Request\\d.csv", "", .) %>% tolower()
  
  f_content <- read.csv(paste0(data_path, "/", f), stringsAsFactors=FALSE) %>% 
               dplyr::mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
 
  
  # f_content <- dplyr::rowwise(f_content) %>% 
  #              dplyr::mutate(Next_Pos_date = suppressWarnings(Positions_dates[min(which(Positions_dates > Date))])) %>%
  #              dplyr::ungroup()
  
  if(dbExistsTable(con, data_field)) dbWriteTable(con, data_field, f_content,  append = T , overwrite = F)
  else dbWriteTable(con, data_field, f_content)
  
  # names(f_content)[which(names(f_content) == "Date")] <- paste0("Date_", data_field)
  # if(NCOL(f_content) > 3) names(f_content)[which(names(f_content) == "Value")] <- data_field
  # # Constructing the dataset
  # data.set <<- dplyr::left_join(data.set, f_content, by = c("Symbol", "Date" = "Next_Pos_date"))
}

## Construction & alignment of variables in dataset
data.set <- dbReadTable(con, "SharkPositions_good") %>% dplyr::mutate(Date = as.Date(Date))
Positions_dates <- sort(unique(data.set$Date))
# saveRDS(data.set, file = "DataSet.rds")


tbls_list <- dbListTables(con)
counter <- 0

# To get memory used
pryr::mem_used()

EMA_irregularTS <- function(df, min.num.pts) {
  browser()
  View(df)
  xts.df <- xts::xts(df$Value, order.by = df$Date)
  tt_DEMA <- TTR::DEMA(xts.df, n = 4)
  tt_ZLEMA <- TTR::ZLEMA(xts.df, n = 4)
  
  # tt_fit <- forecast::auto.arima(xts.df)
  plot(index(xts.df), xts.df,col="red")
  lines(index(xts.df), tt_ZLEMA ,col="blue")
  lines(index(xts.df), tt_DEMA,col="black")
  browser()
}

ProcessFundamental <- function(df, fn = NULL, min.num.pts = 1, col_name = "Value") {
  # Check if the variable exist in the table.
  if(is.null(fn) | !(col_name %in% names(df))) return(df)
  # Check if the function exist in the workspace to apply it
  if(!exists(fn)) {
    message("Supplied Function does not exist in the workspace")
    return(df)
  }
  dots <- list(lazyeval::interp(~ f(x, y), 
               .values = list(f = as.name(fn), x = as.name("."), y = as.name(min.num.pts))))
  res <- df %>% dplyr::group_by(Symbol) %>% dplyr::do_(.dots = setNames(dots, "ProcessedValue"))
}

for(t in tbls_list) {
  # Verbose msg
  counter <<- counter + 1
  #data.set <<- readRDS("DataSet.rds")
  
  message(paste0("Processing table number: ", counter, ". TableName/Fundamental data: ", t, ". Memory used: ", pryr::mem_used()))
  # Looping through all the tables in the db excluding the sharkpositions table
  if (t %in% c("sharkpositions", "sharkpositions_good", "SharkPositions" , "SharkPositions_good" , "data_set",
               "sec_ticker_exchange", "own_inst_hist")) next
  data.X <- dbReadTable(con, t) %>% dplyr::mutate(Date = as.Date(Date)) %>%
            dplyr::rowwise() %>% 
            dplyr::mutate(Next_Pos_date = suppressWarnings(Positions_dates[min(which(Positions_dates > Date))])) %>%
            dplyr::ungroup() %>%
            # Keeping only 1 data point per symbol per Position Date
            dplyr::arrange(Symbol, desc(Date)) %>%
            dplyr::distinct(Symbol, Next_Pos_date, .keep_all = TRUE)
  # https://gist.github.com/steadyfish/ccb0896b1fa10f8c2528#file-dplyr_functions_programmatic_use-r
  # data.X_Processed <- ProcessFundamental(data.X, fn = NULL)
  # data.X_Processed <- ProcessFundamental(data.X, fn = "EMA_irregularTS")
  names(data.X)[which(names(data.X) == "Date")] <- paste0("Date_", t)
  if(NCOL(data.X) > 3) names(data.X)[which(names(data.X) == "Value")] <- t
  # Constructing the dataset
  data.set <<- dplyr::left_join(data.set, data.X, by = c("Symbol", "Date" = "Next_Pos_date"))
  #saveRDS(data.set, file = "DataSet.rds")
  names(data.set)
  rm(data.X)
  gc()
}

data.set <- data.set %>% dplyr::arrange(Symbol, Date)

data.set2 <- data.set %>% dplyr::mutate(Position_normalized = Position/NumberHolders,
                                 Position_percent = Position_normalized/SharesOutstanding) %>% 
             dplyr::group_by(Symbol) %>% dplyr::mutate(lag_Position_normalized = lag(Position_normalized),
                                                      Position_normalized_change = Position_normalized - lag_Position_normalized,
                                                      
                                                      lag_Position_percent = lag(Position_percent),
                                                      Position_percent_change = Position_percent - lag_Position_percent,
                                                      
                                                      lag_Position = lag(Position),
                                                      Position_change =  Position - lag_Position) %>%
             dplyr::ungroup() %>%
             dplyr::select(-grep(pattern = "Date_", x = names(.)),
                           -grep(pattern = "lag_", x = names(.)),
                           -grep(pattern = "row_names.", x = names(.)))

dbWriteTable(con, "data_set", data.set2, overwrite = TRUE)
data.set2 <- dbReadTable(con, "data_set")
data.set3 <- na.omit(data.set2)

pts_per_symbol <- dplyr::group_by(data.set3, Symbol) %>% dplyr::summarise(Count = n()) %>% dplyr::arrange()
symbol_30 <- dplyr::filter(pts_per_symbol, Count >= 30)

data.set4 <- dplyr::semi_join(data.set3, symbol_30, by = "Symbol")


# Features Selection - TBD
# ref:
# http://stats.stackexchange.com/questions/56092/feature-selection-packages-in-r-which-do-both-regression-and-classification
# http://miningthedetails.com/blog/r/fselector/
# http://www.stat.columbia.edu/~martin/W2024/R10.pdf


# Outliar detection - TBC
# Histogram of Position Change in the training set
hist(Training_data_regression$Position_change, probability = FALSE, breaks = 100)
# Identifying the 50 most extreme position change
idx1 <- which(Training_data_regression$Position_change %in% head(sort(Training_data_regression$Position_change), 25))
idx2 <- which(Training_data_regression$Position_change %in% head(sort(Training_data_regression$Position_change, decreasing = TRUE), 25))
# Plotting the histogram of Position Change after removing the most extreme observation
hist(Training_data_regression$Position_change[-c(idx1, idx2)], probability = FALSE, breaks = 100)
# Plotting density with ggplot
library(ggplot2)
p2 <- ggplot(Training_data_regression, aes(x = Position_change)) +
             geom_density()
p2
# Multivariate approachess
library(HighDimOut)
library(foreach)
library(doParallel)
foreach::getDoParWorkers()
cl <- makeCluster(parallel::detectCores() - 1L, outfile="")
cl <- makeCluster(6L, outfile="")
doParallel::registerDoParallel(cl)
clusterEvalQ(cl, library(foreach))
# http://www.dbs.ifi.lmu.de/~zimek/publications/KDD2010/kdd10-outlier-tutorial.pdf
# https://cran.r-project.org/web/packages/HighDimOut/vignettes/GoldenStateWarriors.html
# As this might be time consumming, we will measure the time take to evaluate
#
# Prior to the implementation of outlier detection algorithms, it is important to normalize the raw data
# http://stackoverflow.com/questions/15215457/standardize-data-columns-in-r

source("./my_ABOD.R")
my_func <- function(x) {
  print(unique(x$Year))
  data_tmp <- x[, !(names(x) %in% c("Date", "Year", "Symbol"))] %>% as.data.frame()
  scaled_data <- scale(x = data_tmp, center = TRUE, scale = TRUE) %>% as.data.frame()
  res.ABOD <- Func.ABOD(data=scaled_data, basic=FALSE, perc=0.1)
  # We are not going to transform the data since"
  # a) only 1 method is used
  # b) some of the angle variance (output values) are 0
  # score.trans.ABOD <- HighDimOut::Func.trans(raw.score = res.ABOD, method = "ABOD")
  # See Func.trans for description and for details:
  # http://www.dbs.ifi.lmu.de/~zimek/publications/SDM2011/SDM11-outlier-preprint.pdf
  score.trans.ABOD <- res.ABOD
  x$ABOD_Score <- score.trans.ABOD
  gc()
  return(x)
}


scanned_data <- data.set3[, c(Y_var, X_var)] %>% # partition() %>% 
                dplyr::group_by(Year) %>% dplyr::do(res = my_func(.))

stopCluster(cl)

library(dplyr)
library(multidplyr)
# Using a user defined function with multidplyr - a pararell version of dplyr
# https://github.com/hadley/multidplyr/issues/14
#
#cluster <- create_cluster(detectCores() - 1L)
#set_default_cluster(cluster)




scaled_data <- scale(x = Training_data_regression, center = TRUE, scale = TRUE) %>% as.data.frame()
names(scaled_data) <- names(Training_data_regression)
# Start the clock!
time_start <- proc.time()
# Detect outliers using the angle based method
res.ABOD <- Func.ABOD(data=scaled_data, basic=FALSE, perc=0.2)
# Stop the clock
time_end <- proc.time()
# Time elapsed
(time_end - time_start)

# Clustering of Data using mixture models
library(mclust)
BIC <- mclustBIC(Training_data_regression$Position_change)
plot(BIC)
summary(BIC)
mod1.1 <- Mclust(Training_data_regression$Position_change, x = BIC)
summary(mod1.1,parameters = TRUE)
plot(mod1.1, what = "classification")

BIC2 <- mclustBIC(Training_data_regression$Position_change[-c(idx1, idx2)])
mod1.2 <- Mclust(Training_data_regression$Position_change, x = BIC2)
summary(mod1.2,parameters = TRUE)
plot(mod1.2, what = "classification")

mod2 <- MclustDA(data = Training_data_classification[,which(names(Training_data_classification) != "Buy_Sell")],
                 class = Training_data_classification[,"Buy_Sell"],
                 modelType = "EDDA")
summary(mod2)

mod3 <- mclustBIC(Training_data_classification[,which(names(Training_data_classification) != "Buy_Sell")])
