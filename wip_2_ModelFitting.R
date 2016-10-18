# Subsetting the dataset for regression/Classification

context_var <- c("Symbol", "Year",  "Date", 
                 "HasOptions", "SharkGrouping", "NumberHolders", "SharesOutstanding", "FSPermSecId")

Y_var_potential <- grep(pattern = "Position", x = names(data.set3), value = TRUE)
Y_var <- "Position_change"
X_var <-  setdiff(names(data.set3),
                  union(Y_var_potential, context_var))

# Identification of Training & Testing data set ------

# Random Sample vs Specific years for training set

RANDOM_TRAINING <- FALSE
FRACTION_TRAINING <- 0.75

if(RANDOM_TRAINING == TRUE) {
  
  n_samples <- floor(NROW(data.set3) * FRACTION_TRAINING)
  sample_ids <- sample.int(n = NROW(data.set3), size = n_samples, replace = FALSE)
  Training_data_regression <- data.set3[sample_ids , c(Y_var, X_var)]
  Test_data_regression     <- data.set3[-sample_ids, c(Y_var, X_var)]
  
} else {
  # Splitting training/Testing
  Years_dataset <- as.numeric(sort(unique(data.set3$Year)))
  Test_years <- c(2015)
  Training_years <- setdiff(Years_dataset, Test_years)
  
  Training_data_regression <- dplyr::filter(data.set3, Year %in% Training_years) %>%
                              dplyr::select_(.dots = c(Y_var, X_var))
  
  Test_data_regression <- dplyr::filter(data.set3, Year %in% Test_years) %>% 
                          dplyr::select_(.dots = c(Y_var, X_var))
}


# Classification data sets -----
Training_data_classification <- Training_data_regression
Training_data_classification$Buy_Sell <- as.factor(ifelse(Training_data_regression[,Y_var] > 0, "Buy", "Sell"))
Training_data_classification <- Training_data_classification[, -which(names(Training_data_classification) == Y_var)]


Test_data_classification <- Test_data_regression
Test_data_classification$Buy_Sell <- as.factor(ifelse(Test_data_regression[,Y_var] > 0, "Buy", "Sell"))
Test_data_classification <- Test_data_classification[, -which(names(Test_data_classification) == Y_var)]




# Plotting the Y vs X to look at the data (Optional but recommended to for possible outlier detection-----------
df_melt <-reshape2::melt(data = data.set3[,c(Y_var, X_var)], "Position_change")

p1 <- ggplot(df_melt,aes(x = value, y = Position_change)) +
      geom_point() +
      facet_wrap(~ variable, scales = "free", ncol = 5)

print(p1)


# Model Fitting ------

# 1. Linear Gaussian Model----------
m_Linear <- lm(Position_change ~ ., data = Training_data_regression)

# 2) Logit/Probit Binomial GLM Model -----

m_Logit <- glm(Buy_Sell ~ ., family=binomial(), data = Training_data_classification)

# When using plain glm, we get a warning: glm.fit: fitted probabilities numerically 0 or 1 occurred
#
# ref:
# http://www.carlislerainey.com/papers/separation.pdf
#

# 3) Trying different proposed model/solution -----
# 3.1 Robust GLM from robustbase package
model_tt1 <- robustbase::glmrob(Buy_Sell ~ ., family=binomial(), data = Training_data_classification)
model_tt2 <- robustbase::glmrob(Buy_Sell ~ ., family=binomial(), data = Training_data_classification, weights.on.x = "robCov")

# 3.2 Bias reduction in binomial-response generalized linear models: brglm package
# penalized likelihood where penalization is by Jeffreys invariant prior.
m_Logit_BiasRedution <- brglm::brglm(Buy_Sell ~ ., family = binomial(), data = Training_data_classification)

# 3.3 Baysian GLM
m_Logit_Bayesian <- arm::bayesglm(Buy_Sell ~ ., family=binomial(link="probit"), data = Training_data_classification)

# 4) Trying smaller model by utilizing the bestglm package -----
# https://rstudio-pubs-static.s3.amazonaws.com/2897_9220b21cfc0c43a396ff9abf122bb351.html
# http://www2.uaem.mx/r-mirror/web/packages/bestglm/vignettes/bestglm.pdf
Xy <- cbind(Training_data_classification[,which(names(Training_data_classification) != "Buy_Sell")], 
            Bell_Sell = Training_data_classification[,"Buy_Sell"])
# Produced error: Error in rep(-Inf, 2^p) : invalid 'times' argument
best_glm_binomial_probit <- bestglm::bestglm(Xy, family=binomial(link="probit"), method = "forward")
# http://stackoverflow.com/questions/12012746/bestglm-alternatives-for-dataset-with-many-variables
rm(Xy)
# 5) Smaller subset of predictor using the glmnet package ------
# ref:
# http://stats.stackexchange.com/questions/77546/how-to-interpret-glmnet
# https://web.stanford.edu/~hastie/Papers/Glmnet_Vignette.pdf
# 
Y <- as.numeric(Training_data_classification[,"Buy_Sell"])
X <- as.matrix(Training_data_classification[,which(names(Training_data_classification) != "Buy_Sell")])
m_glmnet_lasso <- glmnet::glmnet(x = X, y = Y , family = "binomial", alpha = 1) ## lasso regression - Sparse coeff
m_glmnet_ridge <- glmnet::glmnet(x = X, y = Y , family = "binomial", alpha = 0) ## ridge regression - full coeff

N_folds <- 10
fold_id <- sample(1:N_folds,size=length(Y),replace=TRUE)
m_cv_glmnet_lasso <- cv.glmnet(X,Y, foldid=fold_id, alpha=1, family = "binomial", type.measure="class") ## lasso regression - Sparse coeff
m_cv_glmnet_ridge <- cv.glmnet(X,Y, foldid=fold_id, alpha=0, family = "binomial", type.measure="class") ## ridge regression - full coeff
plot(m_cv_glmnet_lasso)
plot(m_cv_glmnet_ridge)

#m_cv_glmnet_lasso$glmnet.fit

GLM_lasso_betas <- coef(m_cv_glmnet_lasso, s = "lambda.min")
Active.Index <- which(GLM_lasso_betas != 0)           # identifies the covariates that are active in the model and
Active.Coefficients <- GLM_lasso_betas[Active.Index]  # shows the coefficients of those covariates


# Model Prediction ------

Results <- Test_data_regression
Results$Buy_Sell <- as.factor(ifelse(Test_data_regression[,Y_var] > 0, "Buy", "Sell"))

# 1) Linear (Gaussian) Model
Y_pred <- predict(m_Linear,  Test_data_regression[, which(names(Test_data_regression) != Y_var)]) %>% as.vector()
Results$pred_linear_Gaussian_Buy_Sell <- Y_pred

# 2) GLM Logistic (plain) Model
Y_pred <- predict(m_Logit,  Test_data_classification[, which(names(Test_data_classification) != "Buy_Sell")],
                  type = "response") %>% as.vector()
Results$pred_plain_logit_Buy_Sell <- Y_pred

# 3) Bias Reduction GLM
Y_pred <- predict(m_Logit_BiasRedution,  Test_data_classification[, which(names(Test_data_classification) != "Buy_Sell")],
                  type = "response") %>% as.vector()
Results$pred_BiasReduction_logit_Buy_Sell <- Y_pred

# 4) Bayesian GLM
Y_pred <- predict(m_Logit_Bayesian,  Test_data_classification[, which(names(Test_data_classification) != "Buy_Sell")],
                  type = "response") %>% as.vector()
Results$pred_Bayesian_logit_Buy_Sell <- Y_pred


# 5) glmnet model
# To get class, use: type="class"; to get probabilities use: type="response"
# http://stackoverflow.com/questions/26806902/how-to-get-probabilities-between-0-and-1-using-glmnet-logistic-regression
x_test <- as.matrix(Test_data_classification[,which(names(Test_data_classification) != "Buy_Sell")])
# Lasso Results
Y_pred = predict(m_cv_glmnet_lasso, s='lambda.min', newx=x_test, type="class")  %>% as.vector()
Results$pred_glmnet_lasso_Buy_Sell <- Y_pred
# Ridge Results
Y_pred = predict(m_cv_glmnet_ridge, s='lambda.min', newx=x_test, type="class")  %>% as.vector()
Results$pred_glmnet_ridge_Buy_Sell <- Y_pred


# Summary of Model Comparaison based on predictions
Results_Summary <- Results
Results_Summary$Correct_pred_LinearGaussian <- ifelse(sign(Results[, Y_var]) == sign(Results$pred_linear_Gaussian_Buy_Sell), TRUE, FALSE)
Results_Summary <- Results_Summary %>% 
                   dplyr::mutate(Correct_pred_Logistic_plain         = ifelse(Buy_Sell == pred_plain_logit_Buy_Sell, TRUE, FALSE),
                                 Correct_pred_Logistic_BiasReduction = ifelse(Buy_Sell == pred_BiasReduction_logit_Buy_Sell, TRUE, FALSE),
                                 Correct_pred_Logistic_Bayesian      = ifelse(Buy_Sell == pred_Bayesian_logit_Buy_Sell, TRUE, FALSE),
                                 Correct_pred_GLMnet_Lasso           = ifelse(Buy_Sell == pred_glmnet_lasso_Buy_Sell, TRUE, FALSE),
                                 Correct_pred_GLMnet_Ridge           = ifelse(Buy_Sell == pred_glmnet_ridge_Buy_Sell, TRUE, FALSE)) %>%
  dplyr::summarise(Count = n(),
                   Count_Buy  = sum(Buy_Sell == "Buy"),
                   Percent_buy = Count_Buy/Count,
                   Count_Sell = sum(Buy_Sell == "Sell"),
                   Percent_sell =  Count_Sell/Count,
                   accuracy_LinearGaussian = sum(Correct_pred_LinearGaussian == TRUE)/Count,
                   accuracy_Logistic_plain = sum(Correct_pred_Logistic_plain == TRUE)/Count,
                   accuracy_GLMnet_lasso   = sum(Correct_pred_GLMnet_Lasso == TRUE)/Count,
                   accuracy_GLMnet_ridge   = sum(Correct_pred_GLMnet_ridge == TRUE)/Count) %>% t()
