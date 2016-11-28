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
DT3 <- DT2 %>% dplyr::rowwise() %>%
       dplyr::mutate(Symbol_tmp = ifelse(grepl(pattern = "-", x = TICKER_EXCHANGE),
                                               unlist(strsplit(TICKER_EXCHANGE, split = "-", fixed = TRUE))[1],
                                               TICKER_EXCHANGE),
                     Symbol = ifelse(grepl(pattern = ".", x = Symbol_tmp,fixed = TRUE),
                                     unlist(strsplit(Symbol_tmp, split = ".", fixed = TRUE))[1],
                                     Symbol_tmp)) %>%
       dplyr::ungroup() %>% dplyr::select(-Symbol_tmp)

# Multidplyr version
# DOES'T WORK yet - 
# library(dplyr)
# library(multidplyr)
# cluster <- create_cluster(7)
# set_default_cluster(cluster)
# DT3 <- DT2 %>% partition() %>% rowwise() %>%
#                mutate(Symbol_tmp = ifelse(grepl(pattern = "-", x = TICKER_EXCHANGE),
#                                     unlist(strsplit(TICKER_EXCHANGE, split = "-", fixed = TRUE))[1],
#                                     TICKER_EXCHANGE),
#                       Symbol = ifelse(grepl(pattern = ".", x = Symbol_tmp,fixed = TRUE),
#                                       unlist(strsplit(Symbol_tmp, split = ".", fixed = TRUE))[1],
#                                       Symbol_tmp)) %>%
#                collect() %>%
#         dplyr::ungroup() %>% dplyr::select(-Symbol_tmp)

# Writting results to disk using fwrite
# https://www.r-bloggers.com/fast-csv-writing-for-r/
#detach("package:data.table", unload=TRUE)             # detaching the package
#remove.packages("data.table")                         # First remove the current version
#install.packages("data.table", type = "source",
#                 repos = "http://Rdatatable.github.io/data.table") # Then install devel version
data.table::fwrite(DT3, "own_13f_holdings_hist_1_with_symbols.csv")
#DT3 <- DT3[, ReportingDate := as.Date(REPORT_DATE)]
dbWriteTable(con, "Inst_Holdings_hist_w_symbols", DT3)


own_basic <- fread("~/R_workspaces/AI_Targetting/Data from Rob/own_basic.txt", "|")
edm_standard_entity <- fread("~/R_workspaces/AI_Targetting/Data from Rob/edm_standard_entity.txt", "|")

DT4 <- dplyr::left_join(DT3,
                        dplyr::select(own_basic, FS_PERM_SEC_ID, Company = FACTSET_ENTITY_ID, ISSUE_TYPE, CAP_GROUP),
                        by = "FS_PERM_SEC_ID")

DT5 <- dplyr::left_join(DT4,
                        dplyr::select(edm_standard_entity, PRIMARY_SIC_CODE, Company = FACTSET_ENTITY_ID,
                                      PRIMARY_SIC_CODE, INDUSTRY_CODE, SECTOR_CODE),
                        by = "Company")

data.table::fwrite(DT5, "own_13f_holdings_hist_1_with_symbols.csv")

DT6 <- dplyr::filter(DT5, CAP_GROUP %in% c("SMALL","MID", "MEGA", "LARGE"))
dbWriteTable(con, "Inst_Holdings_hist_w_symbols_and_SIC", DT6)

# HOLDINGS IN AMERICAN COMPANIES (identified by NYS and NAS in their exchange code)
DT7 <- dplyr::filter(DT6, grepl(pattern = "(-NYS)|(-NAS)", x = TICKER_EXCHANGE) )
data.table::fwrite(DT7, "own_13f_holdings_hist_1_with_symbols_and_sic_US.csv")
dbWriteTable(con, "Inst_Holdings_hist_w_symbols_and_SIC_US", DT7)


# to be revised
ttt2 <- dplyr::filter(DT4, !is.na(SIC_Group1), grepl(pattern = "(-NYS)|(-NAS)", x = TICKER_EXCHANGE) )



symbol_SIC_Groups <- readr::read_csv("~/R_workspaces/AI_Targetting/Data from Rob/symbolSICGroups.csv")
names(symbol_SIC_Groups)[-1] <- paste0("SIC_", names(symbol_SIC_Groups)[-1])

DT4 <- dplyr::left_join(DT3, symbol_SIC_Groups, by = "Symbol")
