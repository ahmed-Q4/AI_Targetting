# Using plain R-code
source('feature_selection_CMIM.r')
features_selected_CMIM_regression <- CMIM.fast(d = Training_data_regression[, which(names(Training_data_regression) != Y_var)], 
                                               y = Training_data_regression[, Y_var])

features_selected_CMIM_classification <- CMIM.fast(d = Training_data_classification[,which(names(Training_data_classification) != "Buy_Sell")], 
                                                   y = Training_data_regression[, "Buy_Sell"])

#Using packages ----
# Fslector Package ------
library(FSelector)
# http://miningthedetails.com/blog/r/fselector/
# https://cran.r-project.org/web/packages/FSelector/FSelector.pdf


features_information.gain_classification <- information.gain(formula = Buy_Sell ~ ., data = Training_data_classification)
features_gain.ratio_classification <- gain.ratio(formula = Buy_Sell ~ ., data = Training_data_classification)
features_randomForest <- random.forest.importance(formula = Buy_Sell ~ ., data = Training_data_classification, importance.type = 1)
# either 1 or 2, specifying the type of importance measure (1=mean decrease inaccuracy, 2=mean decrease in node impurity)

# infotheo package
# To do:
# Figure out how to use condinformation function:
# http://search.r-project.org/library/infotheo/html/condinformation.html
# to do the same as in the plain R-code above http://www.ams.jhu.edu/~yqin/cvrg/feature_selection_commented.r