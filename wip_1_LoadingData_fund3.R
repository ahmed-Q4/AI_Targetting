library(data.table)
library(bit64)
DT1.1 <- fread("~/R_workspaces/AI_Targetting/own_fund/own_fund_holdings_hist_1.txt", sep = "|")
DT1.2 <- fread("~/R_workspaces/AI_Targetting/own_fund/own_fund_holdings_hist_2.txt", sep = "|")
DT1.3 <- fread("~/R_workspaces/AI_Targetting/own_fund/own_fund_holdings_hist_3.txt", sep = "|")

cleanMem <- function(n=10) { for (i in 1:n) invisible(gc()) }

sec_ticker_exchange <- fread("~/R_workspaces/AI_Targetting/Data from Rob/h_security_ticker_exchange.txt", "|")
#dbWriteTable(con, "sec_ticker_exchange", sec_ticker_exchange)

DT2.3 <- dplyr::left_join(DT1.3, 
                          dplyr::select(sec_ticker_exchange, TICKER_EXCHANGE, FREF_SECURITY_TYPE, FS_PERM_SEC_ID),
                          by = "FS_PERM_SEC_ID")

# Clearing workspace
rm(DT1.3)
rm(sec_ticker_exchange)
cleanMem()

# Identifying the symbols from the Ticker_Exchange
DT3.3 <- DT2.3 %>% dplyr::rowwise() %>%
  dplyr::mutate(Symbol_tmp = ifelse(grepl(pattern = "-", x = TICKER_EXCHANGE),
                                    unlist(strsplit(TICKER_EXCHANGE, split = "-", fixed = TRUE))[1],
                                    TICKER_EXCHANGE),
                Symbol = ifelse(grepl(pattern = ".", x = Symbol_tmp,fixed = TRUE),
                                unlist(strsplit(Symbol_tmp, split = ".", fixed = TRUE))[1],
                                Symbol_tmp)) %>%
  dplyr::ungroup() %>% dplyr::select(-Symbol_tmp)

# Clearing workspace
rm(DT2.3)
cleanMem()

own_basic <- fread("~/R_workspaces/AI_Targetting/Data from Rob/own_basic.txt", "|")

DT4.3 <- dplyr::left_join(DT3.3,
                          dplyr::select(own_basic, FS_PERM_SEC_ID, Company = FACTSET_ENTITY_ID, ISSUE_TYPE, CAP_GROUP),
                          by = "FS_PERM_SEC_ID")



rm(DT3.3)
rm(own_basic)
cleanMem()

edm_standard_entity <- fread("~/R_workspaces/AI_Targetting/Data from Rob/edm_standard_entity.txt", "|")

DT5.3 <- dplyr::left_join(DT4.3,
                        dplyr::select(edm_standard_entity, PRIMARY_SIC_CODE, Company = FACTSET_ENTITY_ID,
                                      PRIMARY_SIC_CODE, INDUSTRY_CODE, SECTOR_CODE),
                        by = "Company")
rm(DT4.3)
rm(edm_standard_entity)
cleanMem()

# Non-Micro-Cap
DT6.3 <- dplyr::filter(DT5.3, CAP_GROUP %in% c("SMALL","MID", "MEGA", "LARGE"))

# Focusing on US only positions based on exchange (NYS and NAS)
DT7.3.us <- dplyr::filter(DT6.3, grepl(pattern = "(-NYS)|(-NAS)", x = TICKER_EXCHANGE) )
DT7.3.non_us <- dplyr::filter(DT6.3, !grepl(pattern = "(-NYS)|(-NAS)", x = TICKER_EXCHANGE) )

cleanMem()
# Writing results to database
library(RMySQL)
con <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")
dbWriteTable(con, "fund_us_holdings_hist_w_symbols_and_sic", DT7.3.us, append = TRUE)
dbWriteTable(con, "fund_non_us_holdings_hist_w_symbols_and_sic", DT7.3.non_us, append = TRUE)
dbDisconnect(con)
# backing up tables to RDS files
saveRDS(DT7.3.us, "own_fund_holdings_hist_1_us_part3.rds")
saveRDS(DT7.3.non_us, "own_fund_holdings_hist_1_non_us_part3.rds")
# Clearing the workspace before quitting
rm(list = ls())
#tt3 <- dbGetQuery(con, "SELECT * FROM Inst_Holdings_hist_w_symbols_and_SIC limit 10")
