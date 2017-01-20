# install.packages("aws.s3", repos = c("cloudyr" = "http://cloudyr.github.io/drat"), INSTALL_opts = "--no-multiarch")
library(aws.s3)
library(readr)
library(xts)
library(foreach)

BucketList <- bucketlist() # to get a list of buckets!
# AWS returns only the first 1000 object in a bucket. 
# To get the entire list, we need to repeate the get_bucket function call using marker variable
S3bucket_1 <- get_bucket(bucket = 'q4quant')
files_list_1 <- data.frame(FileName = sapply(S3bucket_1, FUN = function(x){x$Key}), stringsAsFactors = FALSE)
S3bucket_2 <- get_bucket(bucket = 'q4quant', marker = files_list_1$FileName[NROW(files_list_1)])
files_list_2 <- data.frame(FileName = sapply(S3bucket_2, FUN = function(x){x$Key}), stringsAsFactors = FALSE)
# Binding the results into a single dataframe.
file_list <- rbind(files_list_1, files_list_2) %>% dplyr::filter(grepl('_data.csv', FileName))
file_list$VarName <- gsub("_Input_|_data\\.csv", "", file_list$FileName)
file_list$VarName <- gsub("StandardDeviations", "std", file_list$VarName)
file_list$VarName <- gsub("PeerIndex", "Peer", file_list$VarName)
file_list$nChar <- nchar(file_list$VarName)

cleanMem <- function(n=10) { for (i in 1:n) gc() }

fill_and_DwnSample <- function(my.xts, sample_dates) {
  res <- xts:::na.locf.xts(my.xts)
  res <- res[sample_dates, ]
  return(res)
}




# Opening Connection to MySQL
library(RMySQL)
con_RDS <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")

# Setting up parallel processing ------
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
  con_RDS <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")
})

db.tbl_list <- tryCatch(expr = dbListTables(conn = con_RDS), error = function(cond) return(cond))
file_list_remaining <- dplyr::filter(file_list, !(VarName %in% db.tbl_list))
#file_list2use <- file_list
file_list2use <- file_list_remaining

# Symbols <- c()
# Looping through the multiple object and writting them to MYSQL db
#for(i in 1:NROW(file_list2use)){
#  browser()
#for(i in 20:20){
t_start <- Sys.time()
Results_Upload2RDS <- foreach(i = 1:NROW(file_list2use),
           .combine = list, .multicombine = TRUE,
           .maxcombine = NROW(file_list2use) + 1, .export = NULL,
           .packages = c("xts","readr", "aws.s3")) %dopar%  {

  db.tbl_list <- tryCatch(expr = dbListTables(conn = con_RDS), error = function(cond) return(cond))
  if(any(class(db.tbl_list) == "error")){
    con_RDS <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")
    db.tbl_list <- dbListTables(conn = con_RDS)
  }
  #if(file_list2use$VarName[i] %in%  db.tbl_list) return()
  Indicator_data <- read_csv(get_object(file_list2use$FileName[i], bucket = "q4quant"),
                             col_types = cols(Date = col_date(format = "%m/%d/%Y"), Value = col_double())) %>%
                    dplyr::group_by(Date, Symbol) %>% dplyr::summarise(Value = mean(Value, na.rm = TRUE),
                                                                       Count = n())
  
  #if(any(Indicator_data$Count > 1)) print(paste("Duplicate in fileName:", file_list2use$FileName[i]))
  Date_tmp1 <- unique(Indicator_data$Date) %>% 
    timeDate::timeLastDayInQuarter(. , format = "%Y-%m-%d", zone = "", FinCenter = "") %>% unique() %>%
    as.POSIXct %>% as.Date() %>% sort()
  
  xts.QEnds <- xts::xts(X = c(dummy2remove = rep(0, length(Date_tmp1))), order.by = Date_tmp1)
  data_wide <- tidyr::spread(data = dplyr::select(Indicator_data, Date, Symbol, Value), key = Symbol, value = Value)
  data.xts  <- xts(x = data_wide[,-1], order.by = data_wide$Date)
  xts.tmp <- merge(data.xts, xts.QEnds, all = TRUE)
  if(dim(xts.tmp)[2] == dim(data.xts)[2]) {
    # (Missing) Dates to Adjust
    # Date_tmp1[which(!(Date_tmp1 %in% index(data.xts)))] # These dates were not in the original data (prob due to holidays)
    # browser()
    # index2fill <- which(index(xts.tmp) %in% Date_tmp1[which(!(Date_tmp1 %in% index(data.xts)))])
    # xts.tmp[index2fill, ] <- xts.tmp[index2fill - 1, ]
    # #xts.tmp_QEnds <- xts.tmp[Date_tmp1, ]
    res.filledDwnSampled <- lapply(xts.tmp, FUN = fill_and_DwnSample, sample_dates = Date_tmp1)
    xts.filledDwnSampled <- Reduce(merge.xts, x = res.filledDwnSampled)
    # A faster approach might be: 
    # xts.filledDwnSampled <- do.call(cbind.xts, x = res.filledDwnSampled)
    # See links below
    # http://r.789695.n4.nabble.com/What-is-the-difference-between-Reduce-and-do-call-td4677792.html#a4677795 and
    # http://stackoverflow.com/questions/12028671/merging-a-large-list-of-xts-objects
    data.wide_QEnds <- data.frame(Date = index(xts.filledDwnSampled), coredata(xts.filledDwnSampled))
    data.long_QEnds <- data.wide_QEnds %>% 
                       tidyr::gather_(key = "Symbol", gather_cols = names(xts.tmp), value_col = file_list2use$VarName[i]) %>%
                       na.omit()
    if(any(names(xts.tmp) != names(data.xts))){
      #Fixing Symbols Name
      symbol2fix <- which(!(names(xts.tmp) %in% names(data.xts)))
      for(j in symbol2fix) {
        data.long_QEnds <- data.long_QEnds %>% 
                           dplyr::mutate(Symbol = gsub(x = Symbol, pattern = names(xts.tmp)[j], replacement = names(data.xts)[j]))
      }
      # Symbols <<- cbind(Symbols, names(data.xts))
    }
    write_var2db <- tryCatch(expr = dbWriteTable(conn = con_RDS, name = file_list2use$VarName[i], value = data.long_QEnds),
                             error = function(cond) return(cond))
    # Re-connecting to RDS if the connection isn't working
    if(any(class(write_var2db) == "error")) {
      con_RDS <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")
      dbWriteTable(conn = con_RDS, name = file_list2use$VarName[i], value = data.long_QEnds)
    }
    dbSendQuery(conn = con_RDS, paste("ALTER TABLE", file_list2use$VarName[i], "MODIFY Date DATE;"))
    dbSendQuery(conn = con_RDS, paste("ALTER TABLE", file_list2use$VarName[i], "ADD CONSTRAINT PIndex PRIMARY KEY PIndex (`Date`, Symbol(10));"))
    #print(paste("Uploaded ", file_list2use$FileName[i]))
    if(any(Indicator_data$Count > 1)) status <- paste("Duplicate in fileName:", file_list2use$FileName[i])
    else status <- "good"
  } 
  else {
    #print(paste("Problem with", file_list2use$FileName[i]))
    status <- "bad"
  }
  res <- list(file_list2use$FileName[i], status, names(data.xts))
  
  rm(Indicator_data, res.filledDwnSampled, data_wide, data.xts, xts.tmp)
  cleanMem()
  return(res)
}
t_end <- Sys.time()

clusterEvalQ(cl, {
  dbDisconnect(conn = con_RDS)
  rm(con_RDS)
})

clusterEvalQ(cl, {
  #ls()
  rm(list = ls())
  #search()
})
stopCluster(cl)
rm(cl)


FilesWithDuplicates <- sapply(Results_Upload2RDS, function(x) x[[2]])
