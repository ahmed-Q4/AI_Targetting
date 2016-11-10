library(data.table)
library(bit64)
DT <- fread("~/R_workspaces/AI_Targetting/own_13f_holdings_hist_1.txt", sep = "|")
DT$REPORT_DATE <- as.Date(DT$REPORT_DATE)
#pryr::mem_used()

library(RMySQL)
con <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")
dbWriteTable(con, "Inst_Holdings_hist", DT)

sec_ticker_exchange <- fread("~/R_workspaces/AI_Targetting/Data from Rob/h_security_ticker_exchange.txt", "|")
#dbWriteTable(con, "sec_ticker_exchange", sec_ticker_exchange)
DT2 <- dplyr::left_join(DT, 
                        dplyr::select(sec_ticker_exchange, TICKER_EXCHANGE, FREF_SECURITY_TYPE, FS_PERM_SEC_ID),
                        by = "FS_PERM_SEC_ID")
DT2 <- DT2 %>% dplyr::rowwise() %>% 
       dplyr::mutate(Symbol_tmp = ifelse(grepl(pattern = "-", x = TICKER_EXCHANGE),
                                               strsplit(TICKER_EXCHANGE, split = "-")[[1]],
                                               TICKER_EXCHANGE),
                     Symbol = ifelse(grepl(pattern = ".", x = Symbol_tmp),
                                     strsplit(Symbol_tmp, split = ".")[[1]],
                                     Symbol_tmp)) %>%
       dplyr::ungroup() %>% dplyr::select(-Symbol_tmp)

# Writting results to disk using fwrite
# https://www.r-bloggers.com/fast-csv-writing-for-r/
#detach("package:data.table", unload=TRUE)             # detaching the package
#remove.packages("data.table")                         # First remove the current version
#install.packages("data.table", type = "source",
#                 repos = "http://Rdatatable.github.io/data.table") # Then install devel version
data.table::fwrite(DT2, "security_ticker_exchange_with_symbols.csv")
#DT3 <- DT3[, ReportingDate := as.Date(REPORT_DATE)]
dbWriteTable(con, "Inst_Holdings_hist_w_symbols", DT2)

symbol_SIC_Groups <- read_csv("~/R_workspaces/AI_Targetting/Data from Rob/symbolSICGroups.csv")
names(symbol_SIC_Groups)[-1] <- paste0("SIC_", names(symbol_SIC_Groups)[-1])
DT3 <- dplyr::left_join(DT2, symbol_SIC_Groups, by = "Symbol")