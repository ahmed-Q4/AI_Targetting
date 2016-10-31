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
# source("SupportingFn.R")
# perf_plot(mod = m_Logit, y = Training_data_classification[,"Buy_Sell"])

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
            Buy_Sell = Training_data_classification[,"Buy_Sell"])
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
# It is known that the ridge penalty shrinks the coefficients of correlated predictors towards each other while the
# lasso tends to pick one of them and discard the others. The elastic-net penalty mixes these two; if predictors
# are correlated in groups, an α = 0.5 tends to select the groups in or out together. This is a higher level
# parameter, and users might pick a value upfront, else experiment with a few different values. One use of α is
# for numerical stability; for example, the elastic net with α = 1 − ε for some small ε > 0 performs much like
# the lasso, but removes any degeneracies and wild behavior caused by extreme correlations.

m_glmnet_lasso <- glmnet::glmnet(x = X, y = Y , family = "binomial", alpha = 1) ## lasso regression - Sparse coeff
m_glmnet_ridge <- glmnet::glmnet(x = X, y = Y , family = "binomial", alpha = 0) ## ridge regression - full coeff

N_folds <- 10
fold_id <- sample(1:N_folds,size=length(Y),replace=TRUE)
m_cv_glmnet_lasso <- glmnet::cv.glmnet(X,Y, foldid=fold_id, alpha=1, family = "binomial", type.measure="class") ## lasso regression - Sparse coeff
m_cv_glmnet_ridge <- glmnet::cv.glmnet(X,Y, foldid=fold_id, alpha=0, family = "binomial", type.measure="class") ## ridge regression - full coeff
plot(m_cv_glmnet_lasso)
plot(m_cv_glmnet_ridge)

#m_cv_glmnet_lasso$glmnet.fit

GLM_lasso_betas <- coef(m_cv_glmnet_lasso, s = "lambda.min") #  “lambda.min”: the λ at which the minimal MSE is achieved.
Active.Index <- which(GLM_lasso_betas != 0)           # identifies the covariates that are active in the model and
Active.Coefficients <- GLM_lasso_betas[Active.Index]  # shows the coefficients of those covariates

# http://stackoverflow.com/questions/27801130/extracting-coefficient-variable-names-from-glmnet-into-a-data-frame
coeff_lasso_df <- data.frame(coef.name = dimnames(coef(m_cv_glmnet_lasso, s = "lambda.min"))[[1]],
                             coef.value = matrix(coef(m_cv_glmnet_lasso, s = "lambda.min"))) %>%
                  dplyr::filter(coef.value != 0)

# 6) Tree Models - MOB with partykit -----

library(sandwich)
library(partykit)
m_partykit_glmTree <- glmtree(formula = Buy_Sell ~ ., data = Training_data_classification, family = binomial, prune = "BIC")

# Determining the cut off probability threshold to determine the class -----
library(pROC)
# http://stats.stackexchange.com/questions/133320/logistic-regression-class-probabilities
# http://stats.stackexchange.com/questions/25389/obtaining-predicted-values-y-1-or-0-from-a-logistic-regression-model-fit
# https://hopstat.wordpress.com/2014/12/19/a-small-introduction-to-the-rocr-package/
#
# if your classifier were aiming to evaluate a diagnostic test for a serious disease that has a relatively safe cure, 
# the sensitivity is far more important that the specificity. 
# In another case, if the disease were relatively minor and the treatment were risky, specificity would be more important to control. 
# For general classification problems, it is considered "good" to jointly optimize the sensitivity and specification.
# 
# The threshold can be determined in a variety of way. Check the OptimalCutpoints package
# http://stats.stackexchange.com/questions/29719/how-to-determine-best-cutoff-point-and-its-confidence-interval-using-roc-curve-i


# Specifying the cost function
cost_fn <- function(analysis, sensitivity_cost = 1, specificity_cost = 1) {
  res <- (sensitivity_cost * analysis$sensitivities) + (specificity_cost * analysis$specificities)
  return(res)
}

#apply roc function for Logit model
analysis <- roc(response=Training_data_classification$Buy_Sell, predictor=m_Logit$fitted.values)
e <- cbind(analysis$thresholds, cost_fn(analysis))
opt_t_Logit <- subset(e,e[,2]==max(e[,2]))[,1]

#apply roc function for baysian Logit model
analysis <- roc(response=Training_data_classification$Buy_Sell, predictor=m_Logit_Bayesian$fitted.values)
e <- cbind(analysis$thresholds, cost_fn(analysis))
opt_t_bayes <- subset(e,e[,2]==max(e[,2]))[,1]

#apply roc function for glmnet Lasso model
analysis <- roc(response=Training_data_classification$Buy_Sell, 
                predictor=predict(m_cv_glmnet_lasso,
                                  newx = as.matrix(Training_data_classification[,which(names(Training_data_classification) != "Buy_Sell")]),
                                  s = "lambda.min", type = "response"))

e <- cbind(analysis$thresholds, cost_fn(analysis))
opt_t_glmnet_lasso <- subset(e,e[,2]==max(e[,2]))[,1]

#apply roc function for glmnet Ridge model
analysis <- roc(response=Training_data_classification$Buy_Sell, 
                predictor=predict(m_cv_glmnet_ridge,
                                  newx = as.matrix(Training_data_classification[,which(names(Training_data_classification) != "Buy_Sell")]),
                s = "lambda.min", type = "response"))

e <- cbind(analysis$thresholds, cost_fn(analysis))
opt_t_glmnet_ridge <- subset(e,e[,2]==max(e[,2]))[,1]


#apply roc function for glmTree - partykit
analysis <- roc(response=Training_data_classification$Buy_Sell, 
                predictor=predict(m_partykit_glmTree,
                                  newdata = Training_data_classification[,which(names(Training_data_classification) != "Buy_Sell")],
                                  type = "response"))

e <- cbind(analysis$thresholds, cost_fn(analysis))
opt_t_glmTree <- subset(e,e[,2]==max(e[,2]))[,1]


opt_t_models <- c(logit = opt_t_Logit, Baysian = opt_t_bayes,
                  GLMnet_Lasso = opt_t_glmnet_lasso, GLMnet_Ridge = opt_t_glmnet_ridge,
                  glm_tree = opt_t_glmTree)
# All model yields similar optimal threshold, and so we will average them and use that estimates to decide on the class when needed
opt_t <- mean(opt_t_models)


# Determining the threshold Method 2 ------
library(ROCR)
# https://hopstat.wordpress.com/2014/12/19/a-small-introduction-to-the-rocr-package/
# Logit model
pred_logit <- prediction(predictions = m_Logit$fitted.values, labels = Training_data_classification$Buy_Sell)
cost.perf <- performance(pred_logit, "cost")
cost.perf_assymetric  <- performance(pred_logit, "cost", cost.fp = 2, cost.fn = 1)
cost.perf_assymetric2 <- performance(pred_logit, "cost", cost.fp = 1, cost.fn = 2)
cost.perf_assymetric3 <- performance(pred_logit, "cost", cost.fp = 1, cost.fn = 5)
# Finding optimal threshold which minimize the cost
# Minimizing cost associated with (False Positive, False Negative)
opt_t2 <- pred_logit@cutoffs[[1]][which.min(cost.perf@y.values[[1]])]
opt_t3 <- pred_logit@cutoffs[[1]][which.min(cost.perf_assymetric@y.values[[1]])]
opt_t4 <- pred_logit@cutoffs[[1]][which.min(cost.perf_assymetric2@y.values[[1]])]
opt_t5 <- pred_logit@cutoffs[[1]][which.min(cost.perf_assymetric3@y.values[[1]])]

# To validate whether Buy = 0 OR 1 -------
head(m_Logit$y)
head(Training_data_classification$Buy_Sell)
# From this we can confirm that Buy = 0 and Sell = 1

# Model Prediction for Test Data------
ModelPreictionResults <- function(threshold) {
  # This function access Test_data_regression, Test_data_classification 
  # as well as regression models defined in the global workspace
  
  Results <- Test_data_regression
  Results$Buy_Sell <- as.factor(ifelse(Test_data_regression[,Y_var] > 0, "Buy", "Sell"))
  
  # 1) Linear (Gaussian) Model
  Y_pred <- predict(m_Linear,  Test_data_regression[, which(names(Test_data_regression) != Y_var)]) %>% as.vector()
  Results$pred_linear_Gaussian_Buy_Sell <- Y_pred
  
  # 2) GLM Logistic (plain) Model
  Y_pred <- predict(m_Logit,  Test_data_classification[, which(names(Test_data_classification) != "Buy_Sell")],
                    type = "response") %>% as.vector()
  Results$prob_plain_logit_Buy_Sell <- Y_pred
  Results$pred_plain_logit_Buy_Sell <- ifelse(Y_pred > threshold, "Sell", "Buy")
  
  # 3) Bias Reduction GLM
  Y_pred <- predict(m_Logit_BiasRedution,  Test_data_classification[, which(names(Test_data_classification) != "Buy_Sell")],
                    type = "response") %>% as.vector()
  # For some reason, prediction with this model returns class not prob
  Results$pred_BiasReduction_logit_Buy_Sell <-  ifelse(abs(Y_pred - 1) == .Machine$double.eps, "Sell", "Buy")
  
  # 4) Bayesian GLM
  Y_pred <- predict(m_Logit_Bayesian,  Test_data_classification[, which(names(Test_data_classification) != "Buy_Sell")],
                    type = "response") %>% as.vector()
  Results$prob_Bayesian_logit_Buy_Sell <- Y_pred
  Results$pred_Bayesian_logit_Buy_Sell <- ifelse(Y_pred > threshold, "Sell", "Buy")
  
  
  # 5) glmnet model
  # To get class, use: type="class"; to get probabilities use: type="response"
  # http://stackoverflow.com/questions/26806902/how-to-get-probabilities-between-0-and-1-using-glmnet-logistic-regression
  x_test <- as.matrix(Test_data_classification[,which(names(Test_data_classification) != "Buy_Sell")])
  # Lasso Results
  
  Y_pred = predict(m_cv_glmnet_lasso, s='lambda.min', newx=x_test, type="response")  %>% as.vector()
  Results$prob_glmnet_lasso_Buy_Sell <- Y_pred
  Results$pred_glmnet_lasso_Buy_Sell <- ifelse(Y_pred > threshold, "Sell", "Buy")
  Y_pred = predict(m_cv_glmnet_lasso, s='lambda.min', newx=x_test, type="class")  %>% as.vector()
  Results$pred_glmnet_lasso_Buy_Sell_check <- ifelse(Y_pred == 2, "Sell", "Buy")
  
  # Ridge Results
  Y_pred = predict(m_cv_glmnet_ridge, s='lambda.min', newx=x_test, type="response")  %>% as.vector()
  Results$prob_glmnet_ridge_Buy_Sell <-  Y_pred
  Results$pred_glmnet_ridge_Buy_Sell <- ifelse(Y_pred > threshold, "Sell", "Buy")
  Y_pred = predict(m_cv_glmnet_ridge, s='lambda.min', newx=x_test, type="class")  %>% as.vector()
  Results$pred_glmnet_ridge_Buy_Sell_check <-  ifelse(Y_pred == 2, "Sell", "Buy")
  
  # 6) GLM_tree Results
  Y_pred <- predict(m_partykit_glmTree,
                    newdata = Test_data_classification[,which(names(Test_data_classification) != "Buy_Sell")],
                    type = "response")
  Results$prob_glmTree_logit_Buy_Sell <- Y_pred
  Results$pred_glmTree_logit_Buy_Sell <- ifelse(Y_pred > threshold, "Sell", "Buy")
  
  ## NOTE: ------
  # It seems that GLMnet uses a probability threshold of 0.5, So we will be using the threshold to determine the class.
  
  # Summary of Model Comparaison based on predictions ----
  Results_focus <- Results[,c(1,47:dim(Results)[2])]
  # Pridicted Buy/Sell given Buy/Sell
  Results_focus$pred_Buy_given_Buy_LinearGaussian   <- ifelse(sign(Results$pred_linear_Gaussian_Buy_Sell) > 0 & sign(Results[, Y_var]) > 0, TRUE, FALSE)
  Results_focus$pred_Buy_given_Sell_LinearGaussian  <- ifelse(sign(Results$pred_linear_Gaussian_Buy_Sell) > 0 & sign(Results[, Y_var]) <= 0, TRUE, FALSE)
  Results_focus$pred_Sell_given_Buy_LinearGaussian  <- ifelse(sign(Results$pred_linear_Gaussian_Buy_Sell) <= 0 & sign(Results[, Y_var]) > 0, TRUE, FALSE)
  Results_focus$pred_Sell_given_Sell_LinearGaussian <- ifelse(sign(Results$pred_linear_Gaussian_Buy_Sell) <= 0 & sign(Results[, Y_var]) <= 0, TRUE, FALSE)
  # Buy/Sell given Predicted Buy/Sell
  Results_focus$Buy_given_Pred_Buy_LinearGaussian   <- ifelse(sign(Results[, Y_var]) >  0 & sign(Results$pred_linear_Gaussian_Buy_Sell) >  0, TRUE, FALSE)
  Results_focus$Buy_given_Pred_Sell_LinearGaussian  <- ifelse(sign(Results[, Y_var]) >  0 & sign(Results$pred_linear_Gaussian_Buy_Sell) <= 0, TRUE, FALSE)
  Results_focus$Sell_given_Pred_Buy_LinearGaussian  <- ifelse(sign(Results[, Y_var]) <= 0 & sign(Results$pred_linear_Gaussian_Buy_Sell) >  0, TRUE, FALSE)
  Results_focus$Sell_given_Pred_Sell_LinearGaussian <- ifelse(sign(Results[, Y_var]) <= 0 & sign(Results$pred_linear_Gaussian_Buy_Sell) <= 0, TRUE, FALSE)
  # Accuracy (Correct Prediction)
  Results_focus$Correct_pred_LinearGaussian         <- ifelse(sign(Results[, Y_var]) == sign(Results$pred_linear_Gaussian_Buy_Sell), TRUE, FALSE)
  Results_focus <- Results_focus %>% 
    dplyr::mutate(# Plain Logistic Regression
                  # Predicted Buy/Sell given Buy/Sell
                  pred_Buy_given_Buy_Logistic_plain   = ifelse(pred_plain_logit_Buy_Sell == "Buy"  & Buy_Sell == "Buy", TRUE, FALSE),
                  pred_Buy_given_Sell_Logistic_plain  = ifelse(pred_plain_logit_Buy_Sell == "Buy"  & Buy_Sell == "Sell", TRUE, FALSE),
                  pred_Sell_given_Buy_Logistic_plain  = ifelse(pred_plain_logit_Buy_Sell == "Sell" & Buy_Sell == "Buy", TRUE, FALSE),
                  pred_Sell_given_Sell_Logistic_plain = ifelse(pred_plain_logit_Buy_Sell == "Sell" & Buy_Sell == "Sell", TRUE, FALSE),
                  ## Buy/Sell given Predicted Buy/Sell
                  Buy_given_Pred_Buy_Logistic_plain   = ifelse(Buy_Sell == "Buy"  & pred_plain_logit_Buy_Sell == "Buy" , TRUE, FALSE),
                  Buy_given_Pred_Sell_Logistic_plain  = ifelse(Buy_Sell == "Buy"  & pred_plain_logit_Buy_Sell == "Sell", TRUE, FALSE),
                  Sell_given_Pred_Buy_Logistic_plain  = ifelse(Buy_Sell == "Sell" & pred_plain_logit_Buy_Sell == "Buy", TRUE, FALSE),
                  Sell_given_Pred_Sell_Logistic_plain = ifelse(Buy_Sell == "Sell" & pred_plain_logit_Buy_Sell == "Sell", TRUE, FALSE),
                  # Accuracy
                  Correct_pred_Logistic_plain         = ifelse(pred_plain_logit_Buy_Sell == Buy_Sell, TRUE, FALSE),
                  # Logistic Bias Reduction
                  # Predicted Buy/Sell given Buy/Sell
                  pred_Buy_given_Buy_BiasReduction    = ifelse(pred_BiasReduction_logit_Buy_Sell == "Buy"  & Buy_Sell == "Buy", TRUE, FALSE),
                  pred_Buy_given_Sell_BiasReduction   = ifelse(pred_BiasReduction_logit_Buy_Sell == "Buy"  & Buy_Sell == "Sell", TRUE, FALSE),
                  pred_Sell_given_Buy_BiasReduction   = ifelse(pred_BiasReduction_logit_Buy_Sell == "Sell" & Buy_Sell == "Buy" , TRUE, FALSE),
                  pred_Sell_given_Sell_BiasReduction  = ifelse(pred_BiasReduction_logit_Buy_Sell == "Sell" & Buy_Sell == "Sell", TRUE, FALSE),
                  ## Buy/Sell given Predicted Buy/Sell
                  Buy_given_Pred_Buy_BiasReduction   = ifelse(Buy_Sell == "Buy"  & pred_BiasReduction_logit_Buy_Sell == "Buy" , TRUE, FALSE),
                  Buy_given_Pred_Sell_BiasReduction  = ifelse(Buy_Sell == "Buy"  & pred_BiasReduction_logit_Buy_Sell == "Sell", TRUE, FALSE),
                  Sell_given_Pred_Buy_BiasReduction  = ifelse(Buy_Sell == "Sell" & pred_BiasReduction_logit_Buy_Sell == "Buy" , TRUE, FALSE),
                  Sell_given_Pred_Sell_BiasReduction = ifelse(Buy_Sell == "Sell" & pred_BiasReduction_logit_Buy_Sell == "Sell", TRUE, FALSE),
                  # Accuracy
                  Correct_pred_Logistic_BiasReduction = ifelse(pred_BiasReduction_logit_Buy_Sell == Buy_Sell, TRUE, FALSE),
                  #Baysian Logistic
                  # Predicted Buy/Sell given Buy/Sell
                  pred_Buy_given_Buy_Bayesian    = ifelse(pred_Bayesian_logit_Buy_Sell == "Buy"  & Buy_Sell == "Buy", TRUE, FALSE),
                  pred_Buy_given_Sell_Bayesian   = ifelse(pred_Bayesian_logit_Buy_Sell == "Buy"  & Buy_Sell == "Sell", TRUE, FALSE),
                  pred_Sell_given_Buy_Bayesian   = ifelse(pred_Bayesian_logit_Buy_Sell == "Sell" & Buy_Sell == "Buy", TRUE, FALSE),
                  pred_Sell_given_Sell_Bayesian  = ifelse(pred_Bayesian_logit_Buy_Sell == "Sell" & Buy_Sell == "Sell", TRUE, FALSE),
                  ## Buy/Sell given Predicted Buy/Sell
                  Buy_given_Pred_Buy_Bayesian   = ifelse(Buy_Sell == "Buy"  & pred_Bayesian_logit_Buy_Sell == "Buy" , TRUE, FALSE),
                  Buy_given_Pred_Sell_Bayesian  = ifelse(Buy_Sell == "Buy"  & pred_Bayesian_logit_Buy_Sell == "Sell", TRUE, FALSE),
                  Sell_given_Pred_Buy_Bayesian  = ifelse(Buy_Sell == "Sell" & pred_Bayesian_logit_Buy_Sell == "Buy" , TRUE, FALSE),
                  Sell_given_Pred_Sell_Bayesian = ifelse(Buy_Sell == "Sell" & pred_Bayesian_logit_Buy_Sell == "Sell", TRUE, FALSE),
                  # Accuracy
                  Correct_pred_Logistic_Bayesian = ifelse(pred_Bayesian_logit_Buy_Sell == Buy_Sell, TRUE, FALSE),
                  # GLMnet Lasso
                  # Predicted Buy/Sell given Buy/Sell
                  pred_Buy_given_Buy_Lasso    = ifelse(pred_glmnet_lasso_Buy_Sell == "Buy"  & Buy_Sell == "Buy" , TRUE, FALSE),
                  pred_Buy_given_Sell_Lasso   = ifelse(pred_glmnet_lasso_Buy_Sell == "Buy"  & Buy_Sell == "Sell", TRUE, FALSE),
                  pred_Sell_given_Buy_Lasso   = ifelse(pred_glmnet_lasso_Buy_Sell == "Sell" & Buy_Sell == "Buy" , TRUE, FALSE),
                  pred_Sell_given_Sell_Lasso  = ifelse(pred_glmnet_lasso_Buy_Sell == "Sell" & Buy_Sell == "Sell", TRUE, FALSE),
                  ## Buy/Sell given Predicted Buy/Sell
                  Buy_given_Pred_Buy_Lasso   = ifelse(Buy_Sell == "Buy"  & pred_glmnet_lasso_Buy_Sell == "Buy" , TRUE, FALSE),
                  Buy_given_Pred_Sell_Lasso  = ifelse(Buy_Sell == "Buy"  & pred_glmnet_lasso_Buy_Sell == "Sell", TRUE, FALSE),
                  Sell_given_Pred_Buy_Lasso  = ifelse(Buy_Sell == "Sell" & pred_glmnet_lasso_Buy_Sell == "Buy" , TRUE, FALSE),
                  Sell_given_Pred_Sell_Lasso = ifelse(Buy_Sell == "Sell" & pred_glmnet_lasso_Buy_Sell == "Sell", TRUE, FALSE),
                  # Accuracy
                  Correct_pred_GLMnet_Lasso   = ifelse(pred_glmnet_lasso_Buy_Sell == Buy_Sell, TRUE, FALSE),
                  # GLMnet Ridge
                  #  Predicted Buy/Sell given Buy/Sell
                  pred_Buy_given_Buy_Ridge    = ifelse(pred_glmnet_ridge_Buy_Sell == "Buy" & Buy_Sell == "Buy"  ,TRUE, FALSE),
                  pred_Buy_given_Sell_Ridge   = ifelse(pred_glmnet_ridge_Buy_Sell == "Buy" & Buy_Sell == "Sell" ,TRUE, FALSE),
                  pred_Sell_given_Buy_Ridge   = ifelse(pred_glmnet_ridge_Buy_Sell == "Sell" & Buy_Sell == "Buy" ,TRUE, FALSE),
                  pred_Sell_given_Sell_Ridge  = ifelse(pred_glmnet_ridge_Buy_Sell == "Sell" & Buy_Sell == "Sell",TRUE, FALSE),
                  ## Buy/Sell given Predicted Buy/Sell
                  Buy_given_Pred_Buy_Ridge   = ifelse(Buy_Sell == "Buy"  & pred_glmnet_ridge_Buy_Sell == "Buy" , TRUE, FALSE),
                  Buy_given_Pred_Sell_Ridge  = ifelse(Buy_Sell == "Buy"  & pred_glmnet_ridge_Buy_Sell == "Sell", TRUE, FALSE),
                  Sell_given_Pred_Buy_Ridge  = ifelse(Buy_Sell == "Sell" & pred_glmnet_ridge_Buy_Sell == "Buy" , TRUE, FALSE),
                  Sell_given_Pred_Sell_Ridge = ifelse(Buy_Sell == "Sell" & pred_glmnet_ridge_Buy_Sell == "Sell", TRUE, FALSE),
                  # Accuracy
                  Correct_pred_GLMnet_Ridge   = ifelse(pred_glmnet_ridge_Buy_Sell == Buy_Sell, TRUE, FALSE),
                  # glmTRee - party_kit
                  #  Predicted Buy/Sell given Buy/Sell
                  pred_Buy_given_Buy_glmTRee    = ifelse(pred_glmTree_logit_Buy_Sell == "Buy" & Buy_Sell == "Buy"  ,TRUE, FALSE),
                  pred_Buy_given_Sell_glmTRee   = ifelse(pred_glmTree_logit_Buy_Sell == "Buy" & Buy_Sell == "Sell" ,TRUE, FALSE),
                  pred_Sell_given_Buy_glmTRee   = ifelse(pred_glmTree_logit_Buy_Sell == "Sell" & Buy_Sell == "Buy" ,TRUE, FALSE),
                  pred_Sell_given_Sell_glmTRee  = ifelse(pred_glmTree_logit_Buy_Sell == "Sell" & Buy_Sell == "Sell",TRUE, FALSE),
                  ## Buy/Sell given Predicted Buy/Sell
                  Buy_given_Pred_Buy_glmTRee   = ifelse(Buy_Sell == "Buy"  & pred_glmTree_logit_Buy_Sell == "Buy" , TRUE, FALSE),
                  Buy_given_Pred_Sell_glmTRee  = ifelse(Buy_Sell == "Buy"  & pred_glmTree_logit_Buy_Sell == "Sell", TRUE, FALSE),
                  Sell_given_Pred_Buy_glmTRee  = ifelse(Buy_Sell == "Sell" & pred_glmTree_logit_Buy_Sell == "Buy" , TRUE, FALSE),
                  Sell_given_Pred_Sell_glmTRee = ifelse(Buy_Sell == "Sell" & pred_glmTree_logit_Buy_Sell == "Sell", TRUE, FALSE),
                  # Accuracy
                  Correct_pred_glmTRee   = ifelse(pred_glmTree_logit_Buy_Sell == Buy_Sell, TRUE, FALSE))
    # Summarizing the results and computing Probability Statistics
  #browser()
  Results_Summary <- Results_focus %>% 
    dplyr::summarise(# TestData statistics
                     Count = as.integer(n()),
                     Count_Buy  = as.integer(sum(Buy_Sell == "Buy")),
                     Count_Sell = as.integer(sum(Buy_Sell == "Sell")),
                     Percent_buy = paste(round(100 * Count_Buy/Count, digits = 2), "%"),
                     Percent_sell =  paste(round(100 * Count_Sell/Count,digits = 2), "%"),
                     #
                     # LinearGaussian
                     # ==============
                     #
                     # Predicted Buy/Sell given Buy/Sell
                     Prob_pred_Buy_given_Buy_LinearGaussian   = paste(round(100 * sum(pred_Buy_given_Buy_LinearGaussian == TRUE)  /Count_Buy,digits = 2),"%"),
                     Prob_pred_Buy_given_Sell_LinearGaussian  = paste(round(100 * sum(pred_Buy_given_Sell_LinearGaussian == TRUE) /Count_Sell,digits = 2),"%"),
                     Prob_pred_Sell_given_Buy_LinearGaussian  = paste(round(100 * sum(pred_Sell_given_Buy_LinearGaussian == TRUE) /Count_Buy,digits = 2),"%"),
                     Prob_pred_Sell_given_Sell_LinearGaussian = paste(round(100 * sum(pred_Sell_given_Sell_LinearGaussian == TRUE)/Count_Sell,digits = 2),"%"),
                     # Buy/Sell given Predicted Buy/Sell
                     Prob_Buy_given_pred_Buy_LinearGaussian   = paste(round(100 * sum(Buy_given_Pred_Buy_LinearGaussian == TRUE)  /sum(sign(pred_linear_Gaussian_Buy_Sell) > 0 ),digits = 2),"%"),
                     Prob_Buy_given_pred_Sell_LinearGaussian  = paste(round(100 * sum(Buy_given_Pred_Sell_LinearGaussian == TRUE) /sum(sign(pred_linear_Gaussian_Buy_Sell) <= 0),digits = 2),"%"),
                     Prob_Sell_given_pred_Buy_LinearGaussian  = paste(round(100 * sum(Sell_given_Pred_Buy_LinearGaussian == TRUE) /sum(sign(pred_linear_Gaussian_Buy_Sell) > 0 ) ,digits = 2),"%"),
                     Prob_Sell_given_pred_Sell_LinearGaussian = paste(round(100 * sum(Sell_given_Pred_Sell_LinearGaussian == TRUE)/sum(sign(pred_linear_Gaussian_Buy_Sell) <= 0),digits = 2),"%"),
                     # Accuracy
                     Pred_Accuracy_LinearGaussian             = paste(round(100 * sum(Correct_pred_LinearGaussian == TRUE)/Count,digits = 2), "%"),
                     #
                     # Logit Plain
                     # ===========
                     #
                     # Predicted Buy/Sell given Buy/Sell
                     Prob_pred_Buy_given_Buy_Logistic_plain   = paste(round(100 * sum(pred_Buy_given_Buy_Logistic_plain == TRUE)  /Count_Buy,digits = 2),"%"),
                     Prob_pred_Buy_given_Sell_Logistic_plain  = paste(round(100 * sum(pred_Buy_given_Sell_Logistic_plain == TRUE) /Count_Sell,digits = 2),"%"),
                     Prob_pred_Sell_given_Buy_Logistic_plain  = paste(round(100 * sum(pred_Sell_given_Buy_Logistic_plain == TRUE) /Count_Buy,digits = 2),"%"),
                     Prob_pred_Sell_given_Sell_Logistic_plain = paste(round(100 * sum(pred_Sell_given_Sell_Logistic_plain == TRUE)/Count_Sell,digits = 2),"%"),
                     # Buy/Sell given Predicted Buy/Sell
                     Prob_Buy_given_pred_Buy_Logistic_plain   = paste(round(100 * sum(Buy_given_Pred_Buy_Logistic_plain == TRUE)  /sum(pred_plain_logit_Buy_Sell == "Buy" ),digits = 2),"%"),
                     Prob_Buy_given_pred_Sell_Logistic_plain  = paste(round(100 * sum(Buy_given_Pred_Sell_Logistic_plain == TRUE) /sum(pred_plain_logit_Buy_Sell == "Sell"),digits = 2),"%"),
                     Prob_Sell_given_pred_Buy_Logistic_plain  = paste(round(100 * sum(Sell_given_Pred_Buy_Logistic_plain == TRUE) /sum(pred_plain_logit_Buy_Sell == "Buy" ),digits = 2),"%"),
                     Prob_Sell_given_pred_Sell_Logistic_plain = paste(round(100 * sum(Sell_given_Pred_Sell_Logistic_plain == TRUE)/sum(pred_plain_logit_Buy_Sell == "Sell"),digits = 2),"%"),
                     # Accuracy
                     Pred_Accuracy_Logistic_plain             = paste(round(100 * sum(Correct_pred_Logistic_plain == TRUE)/Count,digits = 2), "%"),
                     #
                     # Logistic Bias Reduction
                     # =======================
                     #
                     # Predicted Buy/Sell given Buy/Sell
                     Prob_pred_Buy_given_Buy_BiasReduction   = paste(round(100 * sum(pred_Buy_given_Buy_BiasReduction == TRUE)  /Count_Buy,digits = 2),"%"),
                     Prob_pred_Buy_given_Sell_BiasReduction  = paste(round(100 * sum(pred_Buy_given_Sell_BiasReduction == TRUE) /Count_Sell,digits = 2),"%"),
                     Prob_pred_Sell_given_Buy_BiasReduction  = paste(round(100 * sum(pred_Sell_given_Buy_BiasReduction == TRUE) /Count_Buy,digits = 2),"%"),
                     Prob_pred_Sell_given_Sell_BiasReduction = paste(round(100 * sum(pred_Sell_given_Sell_BiasReduction == TRUE)/Count_Sell,digits = 2),"%"),
                     # Buy/Sell given Predicted Buy/Sell
                     Prob_Buy_given_pred_Buy_BiasReduction   = paste(round(100 * sum(Buy_given_Pred_Buy_BiasReduction == TRUE)  /sum(pred_BiasReduction_logit_Buy_Sell == "Buy" ),digits = 2),"%"),
                     Prob_Buy_given_pred_Sell_BiasReduction  = paste(round(100 * sum(Buy_given_Pred_Sell_BiasReduction == TRUE) /sum(pred_BiasReduction_logit_Buy_Sell == "Sell"),digits = 2),"%"),
                     Prob_Sell_given_pred_Buy_BiasReduction  = paste(round(100 * sum(Sell_given_Pred_Buy_BiasReduction == TRUE) /sum(pred_BiasReduction_logit_Buy_Sell == "Buy" ),digits = 2),"%"),
                     Prob_Sell_given_pred_Sell_BiasReduction = paste(round(100 * sum(Sell_given_Pred_Sell_BiasReduction == TRUE)/sum(pred_BiasReduction_logit_Buy_Sell == "Sell"),digits = 2),"%"),
                     # Accuracy
                     Pred_Accuracy_BiasReduction             = paste(round(100 * sum(Correct_pred_Logistic_BiasReduction == TRUE)/Count,digits = 2), "%"),
                     #
                     # Baysian Logistic
                     # ================
                     #
                     # Predicted Buy/Sell given Buy/Sell
                     Prob_pred_Buy_given_Buy_Bayesian   = paste(round(100 * sum(pred_Buy_given_Buy_Bayesian == TRUE)  /Count_Buy,digits = 2),"%"),
                     Prob_pred_Buy_given_Sell_Bayesian  = paste(round(100 * sum(pred_Buy_given_Sell_Bayesian == TRUE) /Count_Sell,digits = 2),"%"),
                     Prob_pred_Sell_given_Buy_Bayesian  = paste(round(100 * sum(pred_Sell_given_Buy_Bayesian == TRUE) /Count_Buy,digits = 2),"%"),
                     Prob_pred_Sell_given_Sell_Bayesian = paste(round(100 * sum(pred_Sell_given_Sell_Bayesian == TRUE)/Count_Sell,digits = 2),"%"),
                     # Buy/Sell given Predicted Buy/Sell
                     Prob_Buy_given_pred_Buy_Bayesian   = paste(round(100 * sum(Buy_given_Pred_Buy_Bayesian == TRUE)  /sum(pred_Bayesian_logit_Buy_Sell == "Buy" ),digits = 2),"%"),
                     Prob_Buy_given_pred_Sell_Bayesian  = paste(round(100 * sum(Buy_given_Pred_Sell_Bayesian == TRUE) /sum(pred_Bayesian_logit_Buy_Sell == "Sell"),digits = 2),"%"),
                     Prob_Sell_given_pred_Buy_Bayesian  = paste(round(100 * sum(Sell_given_Pred_Buy_Bayesian == TRUE) /sum(pred_Bayesian_logit_Buy_Sell == "Buy" ),digits = 2),"%"),
                     Prob_Sell_given_pred_Sell_Bayesian = paste(round(100 * sum(Sell_given_Pred_Sell_Bayesian == TRUE)/sum(pred_Bayesian_logit_Buy_Sell == "Sell"),digits = 2),"%"),
                     # Accuracy
                     Pred_Accuracy_Bayesian             = paste(round(100 * sum(Correct_pred_Logistic_Bayesian == TRUE)/Count,digits = 2), "%"),
                     #
                     # GLMnet Lasso
                     # ============
                     #
                     # Predicted Buy/Sell given Buy/Sell
                     Prob_pred_Buy_given_Buy_Lasso   = paste(round(100 * sum(pred_Buy_given_Buy_Lasso == TRUE)  /Count_Buy,digits = 2),"%"),
                     Prob_pred_Buy_given_Sell_Lasso  = paste(round(100 * sum(pred_Buy_given_Sell_Lasso == TRUE) /Count_Sell,digits = 2),"%"),
                     Prob_pred_Sell_given_Buy_Lasso  = paste(round(100 * sum(pred_Sell_given_Buy_Lasso == TRUE) /Count_Buy,digits = 2),"%"),
                     Prob_pred_Sell_given_Sell_Lasso = paste(round(100 * sum(pred_Sell_given_Sell_Lasso == TRUE)/Count_Sell,digits = 2),"%"),
                     # Buy/Sell given Predicted Buy/Sell
                     Prob_Buy_given_pred_Buy_Lasso   = paste(round(100 * sum(Buy_given_Pred_Buy_Lasso == TRUE)  /sum(pred_glmnet_lasso_Buy_Sell == "Buy" ),digits = 2),"%"),
                     Prob_Buy_given_pred_Sell_Lasso  = paste(round(100 * sum(Buy_given_Pred_Sell_Lasso == TRUE) /sum(pred_glmnet_lasso_Buy_Sell == "Sell"),digits = 2),"%"),
                     Prob_Sell_given_pred_Buy_Lasso  = paste(round(100 * sum(Sell_given_Pred_Buy_Lasso == TRUE) /sum(pred_glmnet_lasso_Buy_Sell == "Buy" ),digits = 2),"%"),
                     Prob_Sell_given_pred_Sell_Lasso = paste(round(100 * sum(Sell_given_Pred_Sell_Lasso == TRUE)/sum(pred_glmnet_lasso_Buy_Sell == "Sell"),digits = 2),"%"),
                     # Accuracy
                     Pred_Accuracy_GLMnet_Lasso      = paste(round(100 * sum(Correct_pred_GLMnet_Lasso == TRUE) /Count,digits = 2),"%"),
                     #
                     # GLMnet Ridge
                     # ============
                     #
                     # Predicted Buy/Sell given Buy/Sell
                     Prob_pred_Buy_given_Buy_Ridge   = paste(round(100 * sum(pred_Buy_given_Buy_Ridge == TRUE)  /Count_Buy,digits = 2),"%"),
                     Prob_pred_Buy_given_Sell_Ridge  = paste(round(100 * sum(pred_Buy_given_Sell_Ridge == TRUE) /Count_Sell,digits = 2),"%"),
                     Prob_pred_Sell_given_Buy_Ridge  = paste(round(100 * sum(pred_Sell_given_Buy_Ridge == TRUE) /Count_Buy,digits = 2),"%"),
                     Prob_pred_Sell_given_Sell_Ridge = paste(round(100 * sum(pred_Sell_given_Sell_Ridge == TRUE)/Count_Sell,digits = 2),"%"),
                     # Buy/Sell given Predicted Buy/Sell
                     Prob_Buy_given_pred_Buy_Ridge   = paste(round(100 * sum(Buy_given_Pred_Buy_Ridge == TRUE)  /sum(pred_glmnet_ridge_Buy_Sell == "Buy" ),digits = 2),"%"),
                     Prob_Buy_given_pred_Sell_Ridge  = paste(round(100 * sum(Buy_given_Pred_Sell_Ridge == TRUE) /sum(pred_glmnet_ridge_Buy_Sell == "Sell"),digits = 2),"%"),
                     Prob_Sell_given_pred_Buy_Ridge  = paste(round(100 * sum(Sell_given_Pred_Buy_Ridge == TRUE) /sum(pred_glmnet_ridge_Buy_Sell == "Buy" ),digits = 2),"%"),
                     Prob_Sell_given_pred_Sell_Ridge = paste(round(100 * sum(Sell_given_Pred_Sell_Ridge == TRUE)/sum(pred_glmnet_ridge_Buy_Sell == "Sell"),digits = 2),"%"),
                     # Accuracy
                     Pred_Accuracy_GLMnet_Ridge      = paste(round(100 * sum(Correct_pred_GLMnet_Ridge == TRUE)/Count, digits = 2), "%"),
                     #
                     # glm_Tree
                     # ============
                     #
                     # Predicted Buy/Sell given Buy/Sell
                     Prob_pred_Buy_given_Buy_glmTRee   = paste(round(100 * sum(pred_Buy_given_Buy_glmTRee == TRUE)  /Count_Buy,digits = 2),"%"),
                     Prob_pred_Buy_given_Sell_glmTRee  = paste(round(100 * sum(pred_Buy_given_Sell_glmTRee == TRUE) /Count_Sell,digits = 2),"%"),
                     Prob_pred_Sell_given_Buy_glmTRee  = paste(round(100 * sum(pred_Sell_given_Buy_glmTRee == TRUE) /Count_Buy,digits = 2),"%"),
                     Prob_pred_Sell_given_Sell_glmTRee = paste(round(100 * sum(pred_Sell_given_Sell_glmTRee == TRUE)/Count_Sell,digits = 2),"%"),
                     # Buy/Sell given Predicted Buy/Sell
                     Prob_Buy_given_pred_Buy_glmTRee   = paste(round(100 * sum(Buy_given_Pred_Buy_glmTRee == TRUE)  /sum(pred_glmTree_logit_Buy_Sell == "Buy" ),digits = 2),"%"),
                     Prob_Buy_given_pred_Sell_glmTRee  = paste(round(100 * sum(Buy_given_Pred_Sell_glmTRee == TRUE) /sum(pred_glmTree_logit_Buy_Sell == "Sell"),digits = 2),"%"),
                     Prob_Sell_given_pred_Buy_glmTRee  = paste(round(100 * sum(Sell_given_Pred_Buy_glmTRee == TRUE) /sum(pred_glmTree_logit_Buy_Sell == "Buy" ),digits = 2),"%"),
                     Prob_Sell_given_pred_Sell_glmTRee = paste(round(100 * sum(Sell_given_Pred_Sell_glmTRee == TRUE)/sum(pred_glmTree_logit_Buy_Sell == "Sell"),digits = 2),"%"),
                     # Accuracy
                     Pred_Accuracy_glmTRee      = paste(round(100 * sum(Correct_pred_glmTRee == TRUE)/Count, digits = 2), "%")
                     ) %>% 
    t()
  
  probability_threshold   = paste0("Threshold: ",round(100 * threshold, digits = 2), "%")
  colnames(Results_Summary) <- probability_threshold
  res <- list(Summary = Results_Summary, TestResults = Results_focus, Thresh = threshold)
  return(res)
}


Results_Summary_1 <- ModelPreictionResults(threshold = opt_t )$Summary # Result summary associated with opt_t
Results_Summary_2 <- ModelPreictionResults(threshold = opt_t2)$Summary # Result summary associated with opt_t2
Results_Summary_3 <- ModelPreictionResults(threshold = opt_t3)$Summary # Result summary associated with opt_t3
Results_Summary_4 <- ModelPreictionResults(threshold = opt_t4)$Summary # Result summary associated with opt_t4
Results_Summary_5 <- ModelPreictionResults(threshold = opt_t5)$Summary # Result summary associated with opt_t5
Results_Summary_6 <- ModelPreictionResults(threshold = opt_t_models[5])$Summary # Result summary associated with opt_t for glmTree
Results_Summary_all <- cbind(Results_Summary_5, Results_Summary_4, Results_Summary_2, Results_Summary_1, 
                             Results_Summary_6, Results_Summary_3) %>% as.data.frame()

View(Results_Summary_all)

TestResults <- ModelPreictionResults(threshold = opt_t5 )$TestResults # Result summary associated with opt_t
View(TestResults)
