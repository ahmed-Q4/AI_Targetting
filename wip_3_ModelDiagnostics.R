# Diagnostics

summary(model_Linear_Position_Change) # Model summary
confint(model_Linear_Position_Change, level=0.95) # Confidence Level of Coefficient
# Regression Diagnostic ---- http://www.statmethods.net/stats/rdiagnostics.html
car::qqPlot(model_Linear_Position_Change, main="QQ Plot") #qq plot for studentized residual
# Non-independence of Errors: The durbin WatsonTest Test for Autocorrelated Errors - The null hypothesis (H0) is that there is no correlation among residuals, i.e., they are independent. If the p value is small (close to zero) it means one can reject the null.
car::durbinWatsonTest(model_Linear_Position_Change)
model_Linear_Position_Change_AIC_Selected <- MASS::stepAIC(object = model_Linear_Position_Change, direction = "both")