library(RMySQL)
con_RDS <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "RDS")
con_lcl <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")

Fundamental_data_0 <- dbReadTable(con_lcl, "fundamental_data_Q_Ends") %>% dplyr::mutate(Q_Ends = as.Date(Q_Ends))

sample_fund <- "04BFN4-E"
qry <- paste0("SELECT * FROM fund_us_holdings_hist_w_symbols_and_sic where FACTSET_FUND_ID = '", sample_fund , "'")
data.set_000 <- dbGetQuery(con_lcl, qry)
data.set_00 <- data.set_000 %>% 
                dplyr::mutate(Q_Ends = as.Date(as.character(timeDate::timeLastDayInQuarter(REPORT_DATE, format = "%Y-%m-%d", zone = "", FinCenter = ""))),
                              HOLDING = as.numeric(HOLDING)) %>%
                dplyr::group_by(FACTSET_FUND_ID, TICKER_EXCHANGE, Q_Ends) %>% 
                dplyr::summarise(HOLDING_tot = sum(HOLDING, na.rm = TRUE),
                                 Symbol = dplyr::first(Symbol),
                                 CAP_GROUP = as.factor(dplyr::first(CAP_GROUP)),
                                 SECTOR_CODE = as.factor(dplyr::first(SECTOR_CODE))
                ) %>% dplyr::ungroup()

NROW(data.set_00) #  95160
              
fund_hld_dates_tbl <- dplyr::distinct(data.set_00, Q_Ends)
data.set_0 <- data.set_00 %>% dplyr::group_by(FACTSET_FUND_ID, TICKER_EXCHANGE) %>% 
                dplyr::do(res = add_EntryExit(., Hld_dates_tbl = fund_hld_dates_tbl)) %$%
                dplyr::bind_rows(res)  %>% dplyr::filter(!is.na(HOLDING_tot))

NROW(data.set_0) # 98459

Symbol_list <- unique(data.set_0$Symbol)
Date_list <- unique(data.set_0$Q_Ends)

qry <- paste0("Select * from _tmp_table17 where Symbol IN ('", paste(Symbol_list, collapse = "' , '"), "') ",
              "AND Date IN ('", paste(Date_list, collapse = "' , '"), "');")
#tt_multiple <- dbGetQuery(conn = con_RDS, statement = qry)

qry2 <- paste0("Select * from AverageKurt_120 where Symbol IN ('", paste(Symbol_list, collapse = "' , '"), "') ",
               "AND Date IN ('", paste(Date_list, collapse = "' , '"), "');")

system.time(tt_multiple <- dbGetQuery(conn = con_RDS, statement = qry))
system.time(tt_1 <- dbGetQuery(conn = con_RDS, statement = qry2))

tt_multiple$Date <- as.Date(tt_multiple$Date)
tt_1$Date <- as.Date(tt_1$Date)

data_set_test <- data.set_0 %>%
                # Enriching the ownership data with the fundamental data of the company
                dplyr::left_join(tt_multiple, by = c("Symbol", "Q_Ends" = "Date")) %>%
                dplyr::group_by(TICKER_EXCHANGE) %>% dplyr::arrange(Q_Ends) %>%
                dplyr::mutate(# HOLDING_tot_lag = lag(HOLDING_tot),
                  HOLDING_chng = HOLDING_tot - lag(HOLDING_tot),
                  Buy_Sell = as.factor(ifelse(HOLDING_chng < 0, "Sell", "Buy"))) %>%
                dplyr::ungroup() %>% dplyr::arrange(Symbol, Q_Ends) %>% 
                dplyr::select(-FACTSET_FUND_ID, -TICKER_EXCHANGE,# -Symbol, -Q_Ends,
                              #-HOLDING_tot, -HOLDING_chng,
                              -CAP_GROUP, -SECTOR_CODE, -Enter_position, -Exit_position) %>%
                dplyr::filter(!is.na(Buy_Sell))

nrow(data_set_test) # 94702
# Less than nrow(data_set_0) as some symbols has always been on book, 
# and when calculating position change for any of those symbol, we would always end up with 1 less data point per sample

data_set_test2 <- data_set_test[, c(1, 3, 2, 996, 997, sample(x = 4:NCOL(data_set_test)))]
varLen <- sapply(data_set_test, function(x) length(which(!is.na(x))))
varLen <- data.frame(Variable = names(varLen), varLen)
View(varLen)

# Feature Selection
pLossData <- 0.05
pLossData_incr <- 0.01
retentionRatio <- 1

num_var2consider <- NULL
min_num_var2consider <- 100

while(is.null(num_var2consider) || (num_var2consider < min_num_var2consider) & retentionRatio > 0.5) {
  pLossData <<- pLossData + pLossData_incr
  
  VarLen_reduced <- dplyr::filter(varLen, varLen > (1 - pLossData)*max(varLen)) %>% dplyr::select(Variable) %>%
                    dplyr::filter(!(Variable %in% c("Symbol", "Q_Ends", "HOLDING_tot", "HOLDING_chng"))) %>% unlist()
  
  data_set_test3 <- data_set_test[, sample(x = VarLen_reduced, size = length(VarLen_reduced), replace = FALSE)] %>% na.omit()
  retentionRatio <- nrow(data_set_test3)/nrow(data_set_test)
  num_var2consider <- length(VarLen_reduced)
}

data_set_test2.2 <- data_set_test[, c("Symbol", "Q_Ends", "Buy_Sell", VarLen_reduced) ]

options(java.parameters = "-Xmx12g")

library(FSelector)
library(RWeka)

library(FSelectorRcpp)

info_gain1 <- FSelectorRcpp::information_gain(formula = Buy_Sell ~ ., data = data_set_test3, type = "infogain") %>% 
              tibble::rownames_to_column(var = "PotentialVariable") %>% dplyr::filter(importance != 0)

data_set <- data_set_test3[, c("Buy_Sell", info_gain1$PotentialVariable)]


# VALIDATION SECTION ----------
gain_ratio <- FSelectorRcpp::information_gain(formula = Buy_Sell ~ ., data = data_set_test3, type = "gainratio")
# Rjava issue with memory!!
info_gain2 <- FSelector::information.gain(formula = Buy_Sell ~ ., data = data_set_test3)




info_gain1$scaled_importance <- info_gain1$importance/sum(info_gain1$importance)
info_gain1_sorted <- dplyr::select(info_gain,scaled_importance) %>% tibble::rownames_to_column("Variable") %>% dplyr::top_n(50, scaled_importance)

library(mRMRe) # Can't used since it doesn't handle unordered factors
data4_mRMRe <- mRMR.data(data = data_set_test3)

library(infotheo)
dd <- dplyr::select(data_set_test3,Buy_Sell, StockVolumestd_5 ) %>% discretize(. , disc="equalfreq", nbins=NROW(data_set_test3)^(1/3) )
tt1 <- interinformation(dd)
tt2 <- mutinformation(dd)

library(ggplot2)
ggplot(info_gain_sorted, aes(x = Variable, y = scaled_importance)) + geom_bar(stat = "identity") + coord_flip()

data.set_test_cln <- na.omit(data_set_test)
