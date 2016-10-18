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
data_path <- "./Fundamentals_9_17_2016/fundamentals_9_17_2016"
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
# saveRDS(data.set, file = "DataSet.rds")


tbls_list <- dbListTables(con)
counter <- 0

# To get memory used
pryr::mem_used()

for(t in tbls_list) {
  # Verbose msg
  counter <<- counter + 1
  #data.set <<- readRDS("DataSet.rds")
  
  message(paste0("Processing table number: ", counter, ". TableName/Fundamental data: ", t, ". Memory used: ", pryr::mem_used()))
  # Looping through all the tables in the db excluding the sharkpositions table
  if (t %in% c("sharkpositions", "sharkpositions_good", "SharkPositions" , "SharkPositions_good" , "data_set")) next
  data.X <- dbReadTable(con, t) %>% dplyr::mutate(Date = as.Date(Date)) %>%
            dplyr::rowwise() %>% 
            dplyr::mutate(Next_Pos_date = suppressWarnings(Positions_dates[min(which(Positions_dates > Date))])) %>%
            dplyr::ungroup() %>%
            # Keeping only 1 data point per symbol per Position Date
            dplyr::arrange(Symbol, desc(Date)) %>%
            dplyr::distinct(Symbol, Next_Pos_date, .keep_all = TRUE)
  
  # Renaming the variable before adding them to the data_set
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


# Features Selection - TBD
# ref:
# http://stats.stackexchange.com/questions/56092/feature-selection-packages-in-r-which-do-both-regression-and-classification
# http://miningthedetails.com/blog/r/fselector/
# http://www.stat.columbia.edu/~martin/W2024/R10.pdf


# Outliar detection - TBD
Training_data_regression_ <- OutlierMahdist(Training_data_regression)