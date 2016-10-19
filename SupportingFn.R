# Obtaining predicted values (Y=1 or 0) from a logistic regression model fit

# Method 1 ------
# http://stats.stackexchange.com/questions/25389/obtaining-predicted-values-y-1-or-0-from-a-logistic-regression-model-fit
#
#
perf = function(cut, mod, y)
{
  yhat = (mod$fit>cut)
  w = which(y==1)
  sensitivity = mean( yhat[w] == 1 ) 
  specificity = mean( yhat[-w] == 0 ) 
  c.rate = mean( y==yhat ) 
  d = cbind(sensitivity,specificity)-c(1,1)
  d = sqrt( d[1]^2 + d[2]^2 ) 
  out = t(as.matrix(c(sensitivity, specificity, c.rate,d)))
  colnames(out) = c("sensitivity", "specificity", "c.rate", "distance")
  return(out)
}


perf_plot <- function(mod, y, legend.x = 0, legend.y = 0.25) {
  s = seq(.01,.99,length=1000)
  OUT = matrix(0,1000,4)
  for(i in 1:1000) OUT[i,]=perf(s[i],mod,y)
  # Plotting results
  plot(s,OUT[,1],xlab="Cutoff",ylab="Value",cex.lab=1.5,cex.axis=1.5,ylim=c(0,1),type="l",lwd=2,axes=FALSE,col=2)
  axis(1,seq(0,1,length=5),seq(0,1,length=5),cex.lab=1.5)
  axis(2,seq(0,1,length=5),seq(0,1,length=5),cex.lab=1.5)
  lines(s,OUT[,2],col="darkgreen",lwd=2)
  lines(s,OUT[,3],col=4,lwd=2)
  lines(s,OUT[,4],col="darkred",lwd=2)
  box()
  legend(x = legend.x, y = legend.y,col=c(2,"darkgreen",4,"darkred"),lwd=c(2,2,2,2),c("Sensitivity","Specificity","Classification Rate","Distance"))
}

# Method 2 -----
library(pROC)
#apply roc function
analysis <- roc(response=Training_data_classification$Buy_Sell, predictor=m_Logit$fitted.values)
e <- cbind(analysis$thresholds, analysis$sensitivities+analysis$specificities)
opt_t <- subset(e,e[,2]==max(e[,2]))[,1]

#Plot ROC Curve
plot(1-analysis$specificities,analysis$sensitivities,type="l",
     ylab="Sensitiviy",xlab="1-Specificity",col="black",lwd=2,
     main = "ROC Curve for Logit GLM")
abline(a=0,b=1)
abline(v = opt_t) #add optimal t to ROC curve
opt_t #print t

# Model 2
analysis <- roc(response=Training_data_classification$Buy_Sell, predictor=m_Logit_Bayesian$fitted.values)
e <- cbind(analysis$thresholds,1 * analysis$sensitivities+analysis$specificities)
opt_t <- subset(e,e[,2]==max(e[,2]))[,1]

#Plot ROC Curve
plot(1-analysis$specificities,analysis$sensitivities,type="l",
     ylab="Sensitiviy",xlab="1-Specificity",col="black",lwd=2,
     main = "ROC Curve for Baysian Logit GLM")
abline(a=0,b=1)
abline(v = opt_t) #add optimal t to ROC curve
opt_t

# Method 3 -----
library(ROCR)
# https://hopstat.wordpress.com/2014/12/19/a-small-introduction-to-the-rocr-package/
# Logit model
pred_logit <- prediction(predictions = m_Logit$fitted.values, labels = Training_data_classification$Buy_Sell)

slotNames(pred_logit)

roc.perf = performance(pred_logit, measure = "tpr", x.measure = "fpr")
plot(roc.perf)
abline(a=0, b= 1)
# The further away from the diagonal line, the better.
slotNames(roc.perf)


cost.perf = performance(pred_logit, "cost")
plot(cost.perf)
cost.perf_assymetric = performance(pred_logit, "cost", cost.fp = 2, cost.fn = 1)
plot(cost.perf_assymetric)
acc.perf = performance(pred_logit, measure = "acc")
plot(acc.perf)
MutualInformation.perf = performance(pred_logit, measure = "mi")
plot(MutualInformation.perf)
# Precision/Recall Curve
perf1 <- performance(pred_logit, "prec", "rec")
plot(perf1)

# Determining optimal cutoff point
pred_logit@cutoffs[[1]][which.min(roc.perf@y.values[[1]])]

# Minimizing cost associated with (False Positive, False Negative)
pred_logit@cutoffs[[1]][which.min(cost.perf@y.values[[1]])]
pred_logit@cutoffs[[1]][which.min(cost.perf_assymetric@y.values[[1]])]

# Maximizing accuracy
pred_logit@cutoffs[[1]][which.max(acc.perf@y.values[[1]])]

# the above command in More details
ind = which.max( slot(acc.perf, "y.values")[[1]] )
acc = slot(acc.perf, "y.values")[[1]][ind]
cutoff = slot(acc.perf, "x.values")[[1]][ind]
print(c(accuracy= acc, cutoff = cutoff))



