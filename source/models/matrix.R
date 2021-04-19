library(assertthat)
library(data.table)
library(Matrix)

args <- commandArgs(trailingOnly=TRUE)
n <- length(args)

outcomes_file <- args[1]
feature_files <- args[2:(n-2)]
matrix_file   <- args[n-1]
means_file    <- args[n]
 
print(paste("Adding", outcomes_file))
y <- fread(outcomes_file)

train    <- y$PRIOR_SPEND == 0 & y$SUBSET == "TRAIN"
validate <- y$PRIOR_SPEND == 0 & y$SUBSET == "VALIDATE"
test     <- y$PRIOR_SPEND == 0 & y$SUBSET == "TEST"

n <- length(feature_files)
X_train    <- vector("list", n)
X_validate <- vector("list", n)
X_test     <- vector("list", n)

for (i in 1:n) {
    print(paste("Adding", feature_files[[i]]))
    feature <- fread(feature_files[[i]])
    assert_that(all(y$SAMPLE_ID == feature[,SAMPLE_ID]))
    feature[,SAMPLE_ID:=NULL]
    print(paste(ncol(feature), "features"))
    gc()
    X_train[[i]]    <- Matrix(as.matrix(feature[train,]),    sparse=TRUE)
    gc()
    X_validate[[i]] <- Matrix(as.matrix(feature[validate,]), sparse=TRUE)
    gc()
    X_test[[i]]     <- Matrix(as.matrix(feature[test,]),     sparse=TRUE)
    gc()
}

X_train    <- do.call("cbind", X_train)
gc()
X_validate <- do.call("cbind", X_validate)
gc()
X_test     <- do.call("cbind", X_test)
gc()

save(X_train, X_validate, X_test, y, train, validate, test, file=matrix_file)

write.csv(data.frame(var=colnames(X_train),
		     mean=colMeans(X_train)),
	  row.names=FALSE,
	  file=means_file)
