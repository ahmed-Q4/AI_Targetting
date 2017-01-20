library(RMySQL)
con_RDS <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")
db.tbl_list <- tryCatch(expr = data.frame(tbl_list = dbListTables(conn = con_RDS)), error = function(cond) return(cond)) %>%
                dplyr::filter(!(tbl_list %in% c("_tmp_table0", "_tmp_table17", "_view0")))

# Droping row_names from tables in DBs
for(tbl in db.tbl_list$tbl_list){
  q1 <- paste("ALTER TABLE", tbl, "DROP COLUMN row_names;")
  q_res <- dbGetQuery(conn = con_RDS, statement = q1)
}

# Getting the Counts of unique Dates, Symbols and total counts for table in the db
count_stats <- c()
for(tbl in db.tbl_list$tbl_list){
  print(tbl)
  q2 <- paste("select COUNT(distinct(Date)) as Dates_Count, COUNT(distinct(Symbol)) as Symbols_Count, COUNT(*) as Rows_Count FROM", tbl, ";")
  q_res <- dbGetQuery(conn = con_RDS, statement = q2)
  count_stats <- rbind(count_stats, cbind(tbl, q_res))
}

# Create a View for querrying based on all the tables/indicators in the db
# We wil first create a view from two tables: the table with the largest number of unique Symbol, and the table with the largest numbers of unique dates
# Then we will left join this View, with all the remaining table in the db in order to cover All symbols and All dates in the db.

# Emulate a Full outter join of the table with the most Symbols, and with the most dates.
# Once we do that, we will use left join to bring append all the other tbls, since there isn't a full outter join in MySQL
tbl_largest_symbolCount <- dplyr::filter(count_stats, Symbols_Count == max(Symbols_Count)) %>%
                           dplyr::filter(Dates_Count == max(Dates_Count)) %>% dplyr::select(tbl) %>% unlist()
tbl_largest_DatesCount  <- dplyr::filter(count_stats, Dates_Count == max(Dates_Count)) %>%
                           dplyr::filter(Symbols_Count == max(Symbols_Count)) %>% dplyr::select(tbl) %>% unlist()

# http://stackoverflow.com/questions/4796872/full-outer-join-in-mysql/4796911#4796911
# We r using tbl_largest_symbolCount[1], and tbl_largest_DatesCount[1] in case there is more than 1 such table
q3 <- paste("CREATE TABLE _tmp_table0 AS 
              SELECT Date, Symbol,", tbl_largest_DatesCount[1], ",", tbl_largest_symbolCount[1],
              "FROM", tbl_largest_symbolCount[1],        "LEFT JOIN",tbl_largest_DatesCount[1] , "using(Date, Symbol)",
                   "UNION",
              "SELECT Date, Symbol,",tbl_largest_DatesCount[1], ",", tbl_largest_symbolCount[1],
              "FROM", tbl_largest_symbolCount[1],       "RIGHT JOIN",tbl_largest_DatesCount[1] , "using(Date, Symbol)",
              "order by Date;"
            )

q_res <- dbGetQuery(conn = con_RDS, statement = q3)

tbls_2join <- db.tbl_list %>% dplyr::filter(!(tbl_list %in% c(tbl_largest_symbolCount[1], tbl_largest_DatesCount[1])))
# Creating multiple Views since there is a MySQL has a limit of 60 tbls to join at 1 time.


### NOT COMPLETE!!! Produced only 16 tables and not 18 as expected.
# To fix, See q5 below.
tmp_tbl_index <- 1
q4 <- paste0("CREATE TABLE _tmp_table", tmp_tbl_index, " AS SELECT * from _tmp_table", tmp_tbl_index - 1)
for(i in 1:NROW(tbls_2join)){
  q4 <- paste(q4, "LEFT JOIN", tbls_2join$tbl_list[i], "using(Date, Symbol)")
  if(!(i %% 60)) {
    q_res <- dbGetQuery(conn = con_RDS, statement = q4)
    print(paste("Created tmp tbl #", tmp_tbl_index))
    tmp_tbl_index <<- tmp_tbl_index + 1
    q4 <- paste0("CREATE TABLE _tmp_table", tmp_tbl_index, " AS SELECT * from _tmp_table", tmp_tbl_index - 1)
  }
}
# Creating a table with the remaining Tables if there are any left un-joined!
if(i %% 60) {
  q_res <- dbGetQuery(conn = con_RDS, statement = q4)
  print(paste("Created tmp tbl #", tmp_tbl_index))
}


last_tmp_tbl <- "_tmp_table16"
dbSendQuery(conn = con_RDS, paste("ALTER TABLE", last_tmp_tbl , "ADD CONSTRAINT PIndex PRIMARY KEY PIndex (`Date`, Symbol(10));"))

# Select the colum names from tables
q5 <- paste0("select column_name from information_schema.columns where table_name = '", last_tmp_tbl, "';")
q_res <- dbGetQuery(conn = con_RDS, statement = q5)
tbls_2join_remaining <- dplyr::anti_join(tbls_2join, q_res, by = c("tbl_list" = "column_name"))

tmp_tbl_index <- 18
q6 <- paste0("CREATE TABLE _tmp_table", tmp_tbl_index, " ROW_FORMAT=DYNAMIC AS SELECT * from _tmp_table", tmp_tbl_index - 1)
for(i in 1:NROW(tbls_2join_remaining)){
  q6 <- paste(q6, "LEFT JOIN", tbls_2join_remaining$tbl_list[i], "using(Date, Symbol)")
  if(!(i %% 10)) {
    browser()
    q_res <- dbGetQuery(conn = con_RDS, statement = q6)
    last_tmp_tbl <- paste0("_tmp_table", tmp_tbl_index)
    dbSendQuery(conn = con_RDS, paste("ALTER TABLE", last_tmp_tbl , "ADD CONSTRAINT PIndex PRIMARY KEY PIndex (`Date`, Symbol(10));"))
    print(paste("Created tmp tbl #", tmp_tbl_index))
    tmp_tbl_index <<- tmp_tbl_index + 1
    q6 <- paste0("CREATE TABLE _tmp_table", tmp_tbl_index, " AS SELECT * from _tmp_table", tmp_tbl_index - 1)
  }
}
# Creating a table with the remaining Tables if there are any left un-joined!
if(i %% 60) {
  q_res <- dbGetQuery(conn = con_RDS, statement = q6)
  print(paste("Created tmp tbl #", tmp_tbl_index))
}


