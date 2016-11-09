# See http://www.ams.jhu.edu/~yqin/cvrg/Feature_Selection.htm#Intro
#
# Define a function of f(x)=x*log2(x) log2() is based on 2
#
myxlogx<-function(x)
{
   if (x>0)
   {
      return(x*log2(x))
   }
   else
   {
      # is x<0, then log2(x) is error, so we return 0.
      return(0)
   }
  
}

#
# Estimated entropy of categorical variable x i.e. H(px) where
# px is the empirical probability mass function of x.
#
entropy<-function(x)
{
   #
   # Generate a indicator vector SUBSET, whose individual element SUBSET[i]
   # is a TRUE if x[i] is a valid data, or a FALSE if x[i] is a NA.
   #
   SUBSET<-!is.na(x)
   #
   # Create a table Tx using all the valid data.
   #
   Tx<-table(x[SUBSET])
   #
   # Calculate the probabilities for all the data points.
   #
   px<-Tx/sum(Tx)
   #
   # Calculate the entropy for x.
   #
   -sum(sapply(px,myxlogx))
}
#
# Estimated entropy of a pair of categorical variables x,y
# i.e. for the empirical probability mass function pxy
#
entropy2<-function(x,y)
{
   #
   # Generate a indicator vector SUBSET, whose individual element SUBSET[i]
   # is a TRUE if both of x[i] and y[i] are valid data, or a FALSE if any
   # of x[i] and y[i] are NAs.
   #
   SUBSET<-(!is.na(x))&(!is.na(y))
   #
   # Create a table Txy using all the valid data.
   #
   Txy<-table(x[SUBSET],y[SUBSET])
   #
   # Calculate the probabilities for all the data points.
   #
   pxy<-Txy/sum(Txy)
   #
   # Calculate the entropy for x and y.
   #
   -sum(sapply(pxy,myxlogx))
}
#
# Estimated entropy of a 3-tuple of categorical variables x,y,z
# i.e. for the empirical probability mass function pxyz
#
entropy3<-function(x,y,z)
{
   #
   # Generate a indicator vector SUBSET, whose individual element SUBSET[i]
   # is a TRUE if all of x[i], y[i] and z[i] are valid data, or a FALSE
   # if any of x[i], y[i] and z[i] are NAs.
   #
   SUBSET<-(!is.na(x))&(!is.na(y))&(!is.na(z))
   #
   # Create a table Txyz using all the valid data.
   #
   Txyz<-table(x[SUBSET],y[SUBSET],z[SUBSET])
   #
   # Calculate the probabilities for all the data points.
   #
   pxyz<-Txyz/sum(Txyz)
   #
   # Calculate the entropy for x, y and z.
   #
   -sum(sapply(pxyz,myxlogx))
}
#
#
# Given variables x and y, compute the mutual information.
#
#
MI<-function(x,y)
{
   #
   # Based on formula
   #
   entropy(x)+entropy(y)-entropy2(x,y)
}
#
# Given variables X1,X2,Y, compute the mutual information H({X1,X2},Y}
#
MI2<-function(x1,x2,y)
{
   #
   # Based on formula
   #
   entropy2(x1,x2)+entropy(y)-entropy3(x1,x2,y)
}
#
# Given variables X1,X2,Y, compute the conditional mutual information given Y
#
CMI2<-function(x1,x2,y	)
{
   #
   # Based on formula
   #
   -entropy3(x1,x2,y)-entropy(y)+entropy2(x1,y)+entropy2(x2,y)
}


#
# Fast implementation of CMIM
# 
# Inputs:
#
# d = data frame of features
# y = column of response variable
#
CMIM.fast<-function(d,y)
{
   #
   # Assign N the number of variables in d.
   #
   N<-dim(d)[2]
   #
   # Assign ps (partial score), m (counting indicator), and 
   # nu (selection result) all 0s.
   #
   ps<-rep(0,N)
   m<-rep(0,N)
   nu<-rep(0,N)
   #
   # Assign ps with mutual information between y and every variables in d.
   #
   for (n in seq(1,N))
   {
      ps[n]<-MI(d[,n],y)
   }
   #
   # Assign k from 1 to N, and select the kth feature each time.
   #
   for (k in seq(1,N))
   {
      #
      # Assign sstar a initial value of 0. It is used in the future to 
      # identify the maxium of sorce vector s
      #
      sstar<-0
      #
      # Look for the variable for the kth feature with the maxium 
      # conditional mutual information by assigning n from 1 to N
      #
      for (n in seq(1,N))
      {
         #
         # If s (partial score) is larger than sstar, and m[n] is smaller
         # than k-1, then execute the following program. 
         #
         # This while program will execute only once for each k in 1:N
         # 
         # Since the ps is decreacing, as long as ps[n] is smaller than sstar,
         # then we don't need to update ps any more. That is the reason of
         # (ps[n]>sstar)
         #
         # If we find a ps[n] which is larger than sstar, then we update ps[n]
         # with the all the conditional mutual information of given the 
         # variables we already picked ( from nu[1] to nu[k-1] ). The reason why 
         # (m[n]<k-1) is needed is that the loop need to update all the 
         # variables we picked. from nu[1] to nu [k-1].
         #
         # Note that for k=1, k-1=0, so this part of program is never executed 
         # when k is 1.
         #
         while((ps[n]>sstar)&&(m[n]<k-1))
         {
            #
            # Increace m[n] by 1.
            #
            m[n]<-m[n]+1
            #
            # Update the ps with the minimum of current ps[n] and the
            # conditional mutual information given variable nu[m[n]], where
            # m[n] is between 1 and k-1.
            #
            ps[n]<-min(ps[n],CMI2(d[,n],y,d[,nu[m[n]]]))
         }
         #
         # If, after updating, the ps[n] is still larger than sstar, then
         # we replace the old variable nu[k] with this new variable n, whose ps is
         # larger than the former. And we set sstar to the current ps[n], so
         # we can compare it with the ps of the rest of variables.
         #
         if (ps[n]>sstar)
         {
            #
            # We set sstar to the current ps[n], so that we can compare it
            # with the ps of the rest of variables later.
            #
            sstar<-ps[n]
            #
            # Record the current variable n to the nu vector, which stores 
            # all the feature selection result variables.
            #
            nu[k]<-n
         }
      }
   }
   #
   # List the feature selection result and ps.
   #
   list(nu=nu,ps=ps)
  
}

#
# Standard implementation of CMIM
# 
# Inputs:
#
# d = data frame of features
# y = column of response variable
#
CMIM.standard<-function(d,y)
{
   #
   # Assign N the number of variables in d.
   #
   N<-dim(d)[2]
   #
   # Assign s (score) and nu (selection result) all 0s.
   #
   s<-rep(0,N)
   nu<-rep(0,N)
   #
   # Assign s with mutual information between y and every variables in d.
   #
   for (n in seq(1,N))
   {
      s[n]<-MI(d[,n],y)
   }
   #
   # Assign k from 1 to N, and select the kth feature each time.
   #
   for (k in seq(1,N))
   {
      #
      # Assign nu[k] the variable number (from 1 to N) who maximize the
      # score vectro s.
      #
      nu[k]<-which(s==max(s))
      #
      # Update score vector s by assigning n from 1 to N
      #
      for (n in seq(1,N))
      {
         #
         # update score vector with the minimum of current s and conditional
         # mutual information given the vector we just picked.
         #
         s[n]<-min(s[n],CMI2(d[,n],y,d[,nu[k]]))
      }

   }
   list(nu=nu,s=s)
}

#
# A example
#

# d=read.csv("data.csv",as.is=TRUE)
# f<-d[,52:56]
# y<-d[,51]
# CMIM.fast(f,y)
# CMIM.standard(f,y)
