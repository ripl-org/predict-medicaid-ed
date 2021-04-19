library(gamlr)
library(Matrix)

gammas <- c(0)

auc_cost_capture <- function(predicted, actual) {
    return(mean(cumsum(actual[order(-predicted)]) /
		cumsum(actual[order(-actual)])))
}

args <- commandArgs(trailingOnly=TRUE)

set.seed(strtoi(args[1]))

bootstrap    <- strtoi(args[2])
matrix_file  <- args[3]
coef_file    <- args[4]
predict_file <- args[5]

outcome <- c("COST_EMERGENCE")

# Load matrix
load(matrix_file)
y_train    <- as.vector(y[train,    outcome])
y_validate <- as.vector(y[validate, outcome])

# Generate a bootstrap training sample
set.seed(sample(2147483647, bootstrap+1)[bootstrap+1])
idx <- sample(1:nrow(X_train), nrow(X_train), replace=TRUE)
X_train <- X_train[idx,]
y_train <- y_train[idx]

# Grid search for model with best gamma and lambda
models <- lapply(gammas, function(gamma) {
  model <- gamlr(x=X_train, y=y_train, family="binomial", gamma=gamma, standardize=FALSE)
  model$aucs <- sapply(1:100, function(i) {
    y_predicted <- as.vector(predict(model, newdata=X_validate, type="response", select=i))
    return(auc_cost_capture(y_predicted, y_validate))
  })
  model$auc <- max(model$aucs)
  model$best_lambda <- which.max(model$aucs)
  return(model)
})

best_auc <- max(sapply(models, function(model) { model$auc }))
print(paste("best auc:", best_auc))

best_model <- which.max(lapply(models, function(model) { model$auc }))
model <- models[[best_model]]

best_gamma  <- gammas[best_model]
print(paste("best gamma:", best_gamma))

best_lambda <- model$best_lambda
print(paste("best lambda:", best_lambda, model$lambda[best_lambda]))

# Save regression summary
alpha <- model$alpha[best_lambda]
beta <- model$beta[,best_lambda]
var <- rownames(model$beta)
write.csv(data.frame(var=c(var, "intercept"),
                     coef=c(beta, alpha)),
          row.names=FALSE,
          file=coef_file)

# Predict on test data
predicted <- as.vector(predict(model, newdata=X_test, type="response", select=model$best_lambda))
actual <- as.vector(y[test, outcome])

# Save predictions
write.csv(data.frame(SAMPLE_ID=as.vector(y$SAMPLE_ID[test]),
                     predicted=predicted,
                     actual=actual),
          row.names=FALSE,
          file=predict_file)

