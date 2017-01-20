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
q3 <- paste("CREATE View _view0 AS 
            SELECT Date, Symbol FROM", tbl_largest_symbolCount[1],  "LEFT JOIN",tbl_largest_DatesCount[1], "using(Date, Symbol)",
            "UNION",
            "SELECT Date, Symbol FROM", tbl_largest_symbolCount[1],"RIGHT JOIN",tbl_largest_DatesCount[1] , "using(Date, Symbol)",
            "order by Date;"
)

q_res <- dbGetQuery(conn = con_RDS, statement = q3)

tbls_2join <- db.tbl_list %>% dplyr::filter(!(tbl_list %in% c(tbl_largest_symbolCount[1], tbl_largest_DatesCount[1])))
tbls_2join <- db.tbl_list %>% dplyr::filter(!(tbl_list %in% c("_tmp_table0", "_tmp_table17", "_view0")))
# Creating multiple Views since there is a MySQL has a limit of 60 tbls to join at 1 time.

view_index <- 1
q4 <- paste0("CREATE View view_", formatC(view_index, digits = 2, width = 2, flag = 0), " AS", " SELECT * from _view0")
for(i in 1:NROW(tbls_2join)){
  q4 <- paste(q4, "LEFT JOIN", tbls_2join$tbl_list[i], "using(Date, Symbol)")
  if(!(i %% 60)) {
    q_res <- dbGetQuery(conn = con_RDS, statement = q4)
    print(paste("Created View #", view_index))
    view_index <<- view_index + 1
    q4 <- paste0("CREATE View view_", formatC(view_index, digits = 2, width = 2, flag = 0), " AS SELECT * from _view0")
  }
}
# Creating a table with the remaining Tables if there are any left un-joined!
if(i %% 60) {
  q_res <- dbGetQuery(conn = con_RDS, statement = q4)
  print(paste("Created View #", view_index))
}


---------------------------
tbl_index <- 1
q4 <- paste0("CREATE TABLE _gTable_", formatC(tbl_index, digits = 2, width = 2, flag = 0), " AS", " SELECT * from _view0")
for(i in 1:NROW(tbls_2join)){
  q4 <- paste(q4, "LEFT JOIN", tbls_2join$tbl_list[i], "using(Date, Symbol)")
  if(!(i %% 60)) {
    #q4 <- paste0(q4, ", CONSTRAINT PIndex PRIMARY KEY PIndex (`Date`, Symbol(10));")
    q_res <- dbGetQuery(conn = con_RDS, statement = q4)
    dbSendQuery(conn = con_RDS, paste0("ALTER TABLE _gTable_", formatC(tbl_index, digits = 2, width = 2, flag = 0) ,
                                      " ADD CONSTRAINT PIndex PRIMARY KEY PIndex (`Date`, Symbol(10));"))
    print(paste("Created View #", tbl_index))
    tbl_index <<- tbl_index + 1
    q4 <- paste0("CREATE TABLE _gTable_", formatC(tbl_index, digits = 2, width = 2, flag = 0), " AS SELECT * from _view0")
  }
}
# Creating a table with the remaining Tables if there are any left un-joined!
if(i %% 60) {
  q_res <- dbGetQuery(conn = con_RDS, statement = q4)
  dbSendQuery(conn = con_RDS, paste0("ALTER TABLE _gTable_", formatC(tbl_index, digits = 2, width = 2, flag = 0) ,
                                    " ADD CONSTRAINT PIndex PRIMARY KEY PIndex (`Date`, Symbol(10));"))
  print(paste("Created View #", tbl_index))
}


# TESTING PERFORMANCE BETWEEN USING TABLES AND VIEWS to collect the input data
t_views <- system.time({
  for(i in 1:18){
    q5 <- paste0("Select * from view_", formatC(i, digits = 2, width = 2, flag = 0),
                 " where Symbol IN ('", paste(Symbol_list, collapse = "' , '"), "')",
                 " AND Date IN ('", paste(Date_list, collapse = "' , '"), "');")
    tt <- dbGetQuery(conn = con_RDS, q5)
    if(i == 1) res <- tt
    else res <- dplyr::left_join(res, tt, by = c("Date", "Symbol"))
  }
  
})

t_tbls <- system.time({
  for(i in 1:18){
    q5 <- paste0("Select * from _gTable_", formatC(i, digits = 2, width = 2, flag = 0),
                 " where Symbol IN ('", paste(Symbol_list, collapse = "' , '"), "')",
                 " AND Date IN ('", paste(Date_list, collapse = "' , '"), "');")
    ttt <- dbGetQuery(conn = con_RDS, q5)
    if(i == 1) res <- ttt
    else res <- dplyr::left_join(res, ttt, by = c("Date", "Symbol"))
  }
  
})


tt2 <- dbReadTable(conn = con_RDS, "_tmp_table2")
tt3 <- dbReadTable(conn = con_RDS, "_tmp_table3")

tt <- dplyr::left_join(tt2, tt3, by = c("Date", "Symbol"))
----------------------------------------------------------------------
# Conversting the values from double to decimal(13, 8)
for(tbl in file_list$VarName) {
  print(tbl)
  q6 <- paste("ALTER TABLE", tbl, "MODIFY COLUMN", tbl,"DECIMAL(13,8);")
  dbGetQuery(conn = con_RDS, statement = q6)
}


q3_2 <- paste("CREATE Table _tmp_table_00 AS 
              SELECT Date, Symbol FROM", tbl_largest_symbolCount[1],  "LEFT JOIN",tbl_largest_DatesCount[1], "using(Date, Symbol)",
              "UNION",
              "SELECT Date, Symbol FROM", tbl_largest_symbolCount[1],"RIGHT JOIN",tbl_largest_DatesCount[1] , "using(Date, Symbol)",
              "order by Date;")

q_res <- dbGetQuery(conn = con_RDS, statement = q3_2)
dbSendQuery(conn = con_RDS, paste0("ALTER TABLE _tmp_table_00",
                                   " ADD CONSTRAINT PIndex PRIMARY KEY PIndex (`Date`, Symbol(10));"))



tmp_tbl_index <- 1
q4 <- paste0("CREATE TABLE _tmp_table_", formatC(tmp_tbl_index, digits = 2, width = 2, flag = 0),
             " AS SELECT * from _tmp_table_", formatC(tmp_tbl_index - 1, digits = 2, width = 2, flag = 0))
for(i in 1:NROW(file_list)){
  q4 <- paste(q4, "LEFT JOIN", file_list$VarName[i], "using(Date, Symbol)")
  if(!(i %% 60)) {
    q_res <- dbGetQuery(conn = con_RDS, statement = q4)
    dbSendQuery(conn = con_RDS, paste0("ALTER TABLE _tmp_table_", formatC(tmp_tbl_index, digits = 2, width = 2, flag = 0), 
                                       " ADD CONSTRAINT PIndex PRIMARY KEY PIndex (`Date`, Symbol(10));"))
    print(paste("Created tmp tbl #", tmp_tbl_index))
    tmp_tbl_index <<- tmp_tbl_index + 1
    q4 <- paste0("CREATE TABLE _tmp_table_",  formatC(tmp_tbl_index, digits = 2, width = 2, flag = 0),
                 " AS SELECT * from _tmp_table_", formatC(tmp_tbl_index - 1, digits = 2, width = 2, flag = 0))
  }
}
# Creating a table with the remaining Tables if there are any left un-joined!
if(i %% 60) {
  q_res <- dbGetQuery(conn = con_RDS, statement = q4)
  print(paste("Created tmp tbl #", tmp_tbl_index))
}
