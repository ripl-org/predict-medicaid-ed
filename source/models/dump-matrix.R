library(Matrix)

args <- commandArgs(trailingOnly=TRUE)

in_file         <- args[1]
X_train_file    <- args[2]
X_validate_file <- args[3]
X_test_file     <- args[4]
y_train_file    <- args[5]
y_validate_file <- args[6]
y_test_file     <- args[7]
colnames_file   <- args[8]

load(in_file)

writeMM(X_train,    file=X_train_file)
writeMM(X_validate, file=X_validate_file)
writeMM(X_test,     file=X_test_file)

write.csv(y[train,],    row.names=FALSE, file=y_train_file)
write.csv(y[validate,], row.names=FALSE, file=y_validate_file)
write.csv(y[test, ],    row.names=FALSE, file=y_test_file)

write.csv(colnames(X_train), row.names=FALSE, file=colnames_file)
