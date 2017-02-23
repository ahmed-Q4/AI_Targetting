rf.env <- readRDS("TestingEnv_RF.rds")
for(el in ls(rf.env)) assign(el, get(el, rf.env))


objControl_all <- trainControl(method='cv', number = N_folds, 
                           # returnResamp='none', 
                           #savePredictions = TRUE,
                           # Changed to the below value for conformal prediction.
                           returnResamp = "all",
                           savePredictions = TRUE)

objControl_final <- trainControl(method='cv', number = N_folds, 
                               # returnResamp='none', 
                               #savePredictions = TRUE,
                               # Changed to the below value for conformal prediction.
                               returnResamp = "final",
                               savePredictions = TRUE)

objControl_all_down <- trainControl(method='cv', number = N_folds, sampling = "down",
                                      # returnResamp='none',
                                      #savePredictions = TRUE,
                                      # Changed to the below value for conformal prediction.
                                      returnResamp = "all",
                                      savePredictions = TRUE)

objControl_final_down <- trainControl(method='cv', number = N_folds, sampling = "down",
                            # returnResamp='none',
                            #savePredictions = TRUE,
                            # Changed to the below value for conformal prediction.
                            returnResamp = "final",
                            savePredictions = TRUE)

set.seed(j)
model_all_strata_acc <- train(X_caret, Y_caret, method='rf', metric = "Accuracy", trControl=objControl_all, ntree = 100,
                           ## Tell randomForest to sample by strata.. That means within each class
                           strata = Y_caret, sampsize = rep(min(table(Y_caret)), 2),
                           tuneGrid = expand.grid(.mtry = c(1, 3, 5, 7, 10)),
                           # parameters value for comformal prediction
                           keep.forest=TRUE, predict.all=TRUE, norm.votes=TRUE)
tt_all_strata <- model_all_strata_acc$finalModel$votes
in_pred_all_strata_acc <- predict(model_all_strata_acc, newdata = X_caret)
in_confusion_all_strata_acc <- confusionMatrix(in_pred_all_strata_acc, Y_caret)
out_pred_strata_acc <- predict(model_all_strata_acc, newdata = X_test_caret)
out_confusion_all_strata_acc <- confusionMatrix(out_pred_strata_acc,  Test_data[[Y_Var]])

set.seed(j)
model_all_strata_kappa <- train(X_caret, Y_caret, method='rf', metric = "Kappa", trControl=objControl_all, ntree = 100,
                                ## Tell randomForest to sample by strata.. That means within each class
                                strata = Y_caret, sampsize = rep(min(table(Y_caret)), 2),
                                tuneGrid = expand.grid(.mtry = c(1, 3, 5, 7, 10)),
                                # parameters value for comformal prediction
                                keep.forest=TRUE, predict.all=TRUE, norm.votes=TRUE)

CI_all_strata_kappa <- conformal_prediction_score(model_all_strata_kappa, new_data = X_test_caret[1, ], confidence = 0.9)
CI_all_strata_kappa$obs <- Test_data[[Y_Var]]

in_pred_all_strata_kappa <- predict(model_all_strata_kappa, newdata = X_caret, predict.all=TRUE)
in_confusion_all_strata_kappa <- confusionMatrix(in_pred_all_strata_kappa, Y_caret)
out_pred_strata_kappa <- predict(model_all_strata_kappa, newdata = X_test_caret)
out_confusion_all_strata_kappa <- confusionMatrix(out_pred_strata_kappa, Test_data[[Y_Var]])

in_pred_all_strata_kappa <- predict(model_all_strata_kappa$finalModel, newdata = X_caret, predict.all=TRUE)
in_votes_all_strata_kappa <- apply(in_pred_all_strata_kappa$individual,1,function(x){table(x)}) %>%
                             rbind(obs = Y_caret, pred = in_pred_all_strata_kappa$aggregate)


set.seed(j)
model_final_strata <- train(X_caret, Y_caret, method='rf', metric = "Accuracy", trControl=objControl_final, ntree = 100,
                          ## Tell randomForest to sample by strata.. That means within each class
                          strata = Y_caret, sampsize = rep(min(table(Y_caret)), 2),
                          tuneGrid = expand.grid(.mtry = c(1, 3, 5, 7, 10)),
                          # parameters value for comformal prediction
                          keep.forest=TRUE, predict.all=TRUE, norm.votes=TRUE)
tt_final_strata <- model$finalModel$votes



set.seed(j)
model_all_down_strata_acc <- train(X_caret, Y_caret, method='rf', metric = "Accuracy", trControl=objControl_all_down, ntree = 100,
                                   ## Tell randomForest to sample by strata.. That means within each class
                                   strata = Y_caret, sampsize = rep(min(table(Y_caret)), 2),
                                   tuneGrid = expand.grid(.mtry = c(1, 3, 5, 7, 10)),
                                   # parameters value for comformal prediction
                                   keep.forest=TRUE, predict.all=TRUE, norm.votes=TRUE)

CI_all_down_strata_acc <- conformal_prediction_score(model_all_down_strata_acc, new_data = X_test_caret, confidence = 0.9)
CI_all_down_strata_acc$obs <- Test_data[[Y_Var]]

tt_all_down_strata <- model_all_down_strata_acc$finalModel$votes
in_pred_all_down_strata_acc <- predict(model_all_down_strata_acc, newdata = X_caret)
in_confusion_all_down_strata_acc <- confusionMatrix(in_pred_all_down_strata_acc, Y_caret)
out_pred_down_strata_acc <- predict(model_all_down_strata_acc, newdata = X_test_caret)
out_confusion_all_down_strata_acc <- confusionMatrix(out_pred_down_strata_acc,  Test_data[[Y_Var]])

set.seed(j)
model_all_down_strata_kappa <- train(X_caret, Y_caret, method='rf', metric = "Kappa", trControl=objControl_all_down, ntree = 100,
                                   ## Tell randomForest to sample by strata.. That means within each class
                                   strata = Y_caret, sampsize = rep(min(table(Y_caret)), 2),
                                   tuneGrid = expand.grid(.mtry = c(1, 3, 5, 7, 10)),
                                   # parameters value for comformal prediction
                                   keep.forest=TRUE, predict.all=TRUE, norm.votes=TRUE)

CI_all_down_strata_kappa <- conformal_prediction_score(model_all_down_strata_kappa, new_data = X_test_caret, confidence = 0.9)
CI_all_down_strata_kappa$obs <- Test_data[[Y_Var]]


in_pred_all_down_strata_kappa <- predict(model_all_down_strata_kappa, newdata = X_caret)
in_confusion_all_down_strata_kappa <- confusionMatrix(in_pred_all_down_strata_kappa, Y_caret)
out_pred_down_strata_kappa <- predict(model_all_down_strata_kappa, newdata = X_test_caret)
out_confusion_all_down_strata_kappa <- confusionMatrix(out_pred_down_strata_kappa, Test_data[[Y_Var]])


set.seed(j)
model_final_down_strata <- train(X_caret, Y_caret, method='rf', metric = "Accuracy", trControl=objControl_final_down, ntree = 100,
                               ## Tell randomForest to sample by strata.. That means within each class
                               strata = Y_caret, sampsize = rep(min(table(Y_caret)), 2),
                               tuneGrid = expand.grid(.mtry = c(1, 3, 5, 7, 10)),
                               # parameters value for comformal prediction
                               keep.forest=TRUE, predict.all=TRUE, norm.votes=TRUE)
tt_final_down_strata <- model$finalModel$votes



------------




model4 <- train(X_caret, Y_caret, method='rf', metric = "Accuracy", trControl=objControl, ntree = 100,
                tuneGrid = expand.grid(.mtry = c(1, 3, 5, 7, 10)),
                # parameters value for comformal prediction
                keep.forest=TRUE, predict.all=TRUE, norm.votes=TRUE)

tt4 <- model4$finalModel$votes



set.seed(j)
model5 <- train(X_caret, Y_caret, method='rf', metric = "Accuracy", trControl=objControl, ntree = 100,
                ## Tell randomForest to sample by strata.. That means within each class
                strata = Y_caret,
                tuneGrid = expand.grid(.mtry = c(1, 3, 5, 7, 10)),
                # parameters value for comformal prediction
                keep.forest=TRUE, predict.all=TRUE, norm.votes=TRUE)

tt5 <- model5$finalModel$votes

set.seed(j)
objControl2 <- trainControl(method='cv', number = N_folds, sampling = "down",
                            # returnResamp='none',
                            #savePredictions = TRUE,
                            # Changed to the below value for conformal prediction.
                            returnResamp = "final",
                            savePredictions = TRUE)

model6 <- train(X_caret, Y_caret, method='rf', metric = "Accuracy", trControl=objControl2, ntree = 100,
                ## Tell randomForest to sample by strata.. That means within each class
                strata = Y_caret,
                tuneGrid = expand.grid(.mtry = c(1, 3, 5, 7, 10)),
                # parameters value for comformal prediction
                keep.forest=TRUE, predict.all=TRUE, norm.votes=TRUE)

tt6 <- model6$finalModel$votes

set.seed(j)
model7 <- train(X_caret, Y_caret, method='rf', metric = "Accuracy", trControl=objControl, ntree = 100,
                ## Tell randomForest to sample by strata.. That means within each class
                strata = Y_caret, sampsize = rep(min(table(Y_caret)), 2),
                tuneGrid = expand.grid(.mtry = c(1, 3, 5, 7, 10)),
                # parameters value for comformal prediction
                keep.forest=TRUE, predict.all=TRUE, norm.votes=TRUE)
tt7 <- model7$finalModel$votes

set.seed(j)
objControl3 <- trainControl(method='cv', number = N_folds, sampling = "down",
                            # returnResamp='none',
                            #savePredictions = TRUE,
                            # Changed to the below value for conformal prediction.
                            returnResamp = "all",
                            savePredictions = TRUE)

model8 <- train(X_caret, Y_caret, method='rf', metric = "Accuracy", trControl=objControl3, ntree = 100,
                ## Tell randomForest to sample by strata.. That means within each class
                strata = Y_caret, sampsize = rep(min(table(Y_caret)), 2),
                tuneGrid = expand.grid(.mtry = c(1, 3, 5, 7, 10)),
                # parameters value for comformal prediction
                keep.forest=TRUE, predict.all=TRUE, norm.votes=TRUE)

tt8 <- model8$finalModel$votes