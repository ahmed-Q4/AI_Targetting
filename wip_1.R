# Shark Position Processing

SharkPositions <- read.csv("./distinctSecurities.csv", stringsAsFactors=FALSE) %>% 
                  dplyr::filter(SharkGrouping == "True") %>% dplyr::mutate(Date = as.Date(Date, format = "%m/%d/%Y"),
                                                                           Year = format(Date, "%Y"))

Positions_dates <- sort(unique(SharkPositions$Date))

Transaction_per_year <- dplyr::group_by(SharkPositions, Year) %>% dplyr::summarise(Count = n())
Symbol_per_year <- dplyr::group_by(SharkPositions, Year) %>% dplyr::summarise(Count = length(unique(Symbol)))
transaction_per_symbol <- dplyr::group_by(SharkPositions, Symbol) %>% dplyr::summarise(Count = n()) %>% dplyr::arrange(desc(Count))
transaction_per_symbol_per_year <- dplyr::group_by(SharkPositions, Symbol, Year) %>% dplyr::summarise(Count = n()) %>% dplyr::arrange(desc(Count))
good_symbols_list <- dplyr::filter(transaction_per_symbol_per_year, Count <= 4) %>% dplyr::distinct()
write.csv(transaction_per_symbol_per_year, "Numbers_of_rows_per_symbol_per_year.csv")

# Saving Result to db
library(RMySQL)
con <- dbConnect(RMySQL::MySQL(),  default.file = "~/.my.cnf", group = "local_intel")
dbListTables(con)
dbWriteTable(con, "SharkPositions", SharkPositions)

SharkPositions_good <- data.set <- dplyr::semi_join(SharkPositions, good_symbols_list, by = "Symbol")
dbWriteTable(con, "SharkPositions_good", SharkPositions_good)

# Processing fundamental data and writting them to db.
# data_path <- "~/Data/Fundamentals_9_17_2016/fundamentals_9_17_2016"
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

## Constructing the dataset
data.set <- dbReadTable(con, "SharkPositions_good") %>% dplyr::mutate(Date = as.Date(Date))
# saveRDS(data.set, file = "DataSet.rds")


tbls_list <- dbListTables(con)
counter <- 0

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

# Y_matrix <- data.set3 %>% dplyr::select(Date, Year, Symbol, grep(pattern = "Position", x = names(.)) )
# X_matrix <- data.set3[,c(1,3,9:54)]
# 
# summary(X_matrix)
# summary(Y_matrix)

# Creating a matrix for regression

Test_years <- c(2015)
Training_years <- c(2008:2014)


context_var <- c("Symbol", "Year",  "Date", 
                 "HasOptions", "SharkGrouping", "NumberHolders", "SharesOutstanding", "FSPermSecId")

Y_var_potential <- grep(pattern = "Position", x = names(data.set3), value = TRUE)
Y_var <- "Position_change"
X_var <-  setdiff(names(data.set3),
                  union(Y_var_potential, context_var))

Training_data_regression <- dplyr::filter(data.set3, Year %in% Training_years)
Training_data_regression <- Training_data_regression[, c(Y_var, X_var)]

Test_data_regression <- dplyr::filter(data.set3, Year %in% Test_years)
Test_data_regression <- Test_data_regression[, c(Y_var, X_var)]

#saveRDS(regression_data, "F://Q4 - AI Targetting/regression_data.rds")
# Linear Model ####
model_Linear_Position_Change <- lm(Position_change ~ ., data = Training_data_regression)

summary(model_Linear_Position_Change) # Model summary
confint(model_Linear_Position_Change, level=0.95) # Confidence Level of Coefficient
# Regression Diagnostic ---- http://www.statmethods.net/stats/rdiagnostics.html
car::qqPlot(model_Linear_Position_Change, main="QQ Plot") #qq plot for studentized residual
# Non-independence of Errors: The durbin WatsonTest Test for Autocorrelated Errors - The null hypothesis (H0) is that there is no correlation among residuals, i.e., they are independent. If the p value is small (close to zero) it means one can reject the null.
car::durbinWatsonTest(model_Linear_Position_Change)
model_Linear_Position_Change_AIC_Selected <- MASS::stepAIC(object = model_Linear_Position_Change, direction = "both")
# Logit/Probit Model
Training_data_classification <- Training_data_regression
Training_data_classification$Buy_Sell <- as.factor(ifelse(Training_data_regression[,Y_var] > 0, "Buy", "Sell"))
Training_data_classification <- Training_data_classification[, -which(names(Training_data_classification) == Y_var)]


Test_data_classification <- Test_data_regression
Test_data_classification$Buy_Sell <- as.factor(ifelse(Test_data_regression[,Y_var] > 0, "Buy", "Sell"))
Test_data_classification <- Test_data_classification[, -which(names(Test_data_classification) == Y_var)]


model_Logit_Position_Change <- glm(Buy_Sell ~ ., family=binomial(), data = Training_data_classification)
tt <- brglm(Buy_Sell ~ ., family=binomial(), data = Training_data_classification)
tt3 <- arm::bayesglm(Buy_Sell ~ ., family=binomial(link="probit"), data = Training_data_classification)
summary(tt3)
tt4 <- robustbase::glmrob(Buy_Sell ~ ., family=binomial(link="probit"), data = Training_data_classification)

# Best GLM model/package
Xy <- cbind(Training_data_classification[,which(names(Training_data_classification) != "Buy_Sell")], 
            Bell_Sell = Training_data_classification[,"Buy_Sell"])
best_glm_binomial_probit <- bestglm::bestglm(Xy, family=binomial(link="probit"), method = "forward")

# GLM net:
library(glmnet)
Y <- as.numeric(Training_data_classification[,"Buy_Sell"])
X <- as.matrix(Training_data_classification[,which(names(Training_data_classification) != "Buy_Sell")])
glmnet_model_lasso <- glmnet::glmnet(x = X, y = Y , family = "binomial", alpha = 1) ## lasso regression - Sparse coeff
glmnet_model_ridge <- glmnet::glmnet(x = X, y = Y , family = "binomial", alpha = 0) ## ridge regression - full coeff

foldid=sample(1:10,size=length(Y),replace=TRUE)
CV.glmnet_lasso <- cv.glmnet(X,Y,foldid=foldid,alpha=1, family = "binomial")
plot(CV.glmnet_lasso)
CV.glmnet_lasso$glmnet.fit
small.lambda.betas <- coef(CV.glmnet_lasso, s = "lambda.min")
# To predict and get
x_test <- as.matrix(Test_data_classification[,which(names(Test_data_classification) != "Buy_Sell")])
Y_pred = predict(CV.glmnet_lasso, s='lambda.min', newx=x_test, type="class")  %>% as.vector()
Test_data_classification$Predicted_Buy_Sell <- Y_pred
Test_Summary <- dplyr::mutate(Test_data_classification, Correct_Prediction = ifelse(Buy_Sell == Predicted_Buy_Sell, TRUE, FALSE)) %>%
                dplyr::summarise(Count = n(),
                                 Count_Buy  = sum(Buy_Sell == "Buy"),
                                 Percent_buy = Count_Buy/Count,
                                 Count_Sell = sum(Buy_Sell == "Sell"),
                                 Percent_sell =  Count_Sell/Count,
                                 Percent_accuracy = sum(Correct_Prediction == TRUE)/Count)

# Outliar detection
Training_data_regression_ <- OutlierMahdist(Training_data_regression)


# scatter plot with ggplot2

plotmatrix(Training_data_regression, colour="gray20") + geom_smooth(method="lm")
