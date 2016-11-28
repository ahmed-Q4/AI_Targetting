library(RMySQL)
dbDisconnect(con)
con <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel2")
dbListTables(con)
Sample_funds_bigger <- read.csv("~/Downloads/Sample_funds_bigger.csv", stringsAsFactors=FALSE)
dbWriteTable(con, "fund_us_holdings_hist_w_symbols_and_sic", Sample_funds_bigger )

# Uploading fundamental data to db
data_path <- "./fundamentals_10_15_2016/"
files <- dir(path = data_path, pattern = "_Request")
for(f in files) {
  message(paste("Processing File:", f))
  data_field <- substring(f, 4) %>% gsub(pattern = "_Request\\d.csv", "", .) %>% tolower()
  
  f_content <- read.csv(paste0(data_path, "/", f), stringsAsFactors=FALSE) %>%
    dplyr::mutate(Date = as.Date(Date, format = "%m/%d/%Y"),
                  Q_Ends = as.Date(timeDate::timeLastDayInQuarter(as.character(Date), format = "%Y-%m-%d", zone = "", FinCenter = ""))) 
  
  # Making sure we have only 1 value per Date or Q_end dates per symbol
  # if(NCOL(f_content) > 3) {
  #   f_content <- f_content %>% dplyr::group_by(Symbol, Q_Ends) %>%
  #     dplyr::summarize(Value = mean(Value,na.rm = TRUE)) %>%
  #     dplyr::ungroup()
  # }
  
  if(dbExistsTable(con, data_field)) dbWriteTable(con, data_field, f_content,  append = T , overwrite = F)
  else dbWriteTable(con, data_field, f_content)
  
  # names(f_content)[which(names(f_content) == "Date")] <- paste0("Date_", data_field)
  # if(NCOL(f_content) > 3) names(f_content)[which(names(f_content) == "Value")] <- data_field
  # # Constructing the dataset
  # data.set <<- dplyr::left_join(data.set, f_content, by = c("Symbol", "Date" = "Next_Pos_date"))
}

tbls_list <- dbListTables(con)
Fundamental_data <- NULL
counter <- 0
for(t in tbls_list) {
  # Verbose msg
  counter <<- counter + 1
  message(paste0("Processing table number: ", counter, ". TableName/Fundamental data: ", t, ". Memory used: ", pryr::mem_used()))
  
  # Looping through all the tables in the db excluding the sharkpositions table
  if (t %in% c("fund_us_holdings_hist_w_symbols_and_sic")) next
  data.X <- dbReadTable(con, t) %>% dplyr::mutate(Date = as.Date(Date)) %>% dplyr::select(-Date)
    
  # names(data.X)[which(names(data.X) == "Date")] <- paste0("Date_", t)
  if(NCOL(data.X) >= 3) {
    data.X <- data.X %>% dplyr::group_by(Symbol, Q_Ends) %>%
              dplyr::summarize(Value = mean(Value,na.rm = TRUE)) %>%
              dplyr::ungroup()
    names(data.X)[which(names(data.X) == "Value")] <- t
  # Constructing the dataset
  if(is.null(Fundamental_data)) Fundamental_data <<- data.X
  else Fundamental_data <<- dplyr::full_join(Fundamental_data, data.X, by = c("Symbol", "Q_Ends"))
  }
  #saveRDS(data.set, file = "DataSet.rds")
  print(paste("Number of rows:", NROW(Fundamental_data)))
  rm(data.X)
  gc()
}

dbWriteTable(con, "fundamental_data_Q_Ends", Fundamental_data )
