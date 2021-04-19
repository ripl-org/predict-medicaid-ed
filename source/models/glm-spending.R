library(assertthat)
library(Matrix)

args <- commandArgs(trailingOnly=TRUE)

auc_cost_capture <- function(predicted, actual) {
    return(mean(cumsum(actual[order(-predicted)]) /
		cumsum(actual[order(-actual)])))
}

set.seed(args[1])

matrix_file   <- args[2]
selected_file <- args[3]
coef_file     <- args[4]
resid_file    <- args[5]
validate_file <- args[6]
test_file     <- args[7]

outcome <- c("FUTURE_SPEND")

# Load matrix and selected variables
load(matrix_file)
selected <- read.csv(selected_file, stringsAsFactors=FALSE)$var
print(selected)

X_train <- X_train[,selected]
X_test  <- X_test[,selected]
y_train <- y[train,]

# Check condition number
k <- kappa(X_train, exact=TRUE)
print(paste("condition number (kappa):", k))
assert_that(k < 1000)

# Fit OLS model to log spending, conditional on cost emergence
emergent <- y_train[,outcome] > 0
data <- as.data.frame(as.matrix(X_train[emergent,]))
data$spending <- y_train[emergent, outcome]
model <- lm(spending ~ ., data)
summary(model)

# Save regression coefficients
write.csv(summary(model)$coefficients, file=coef_file)

# Save residuals
write.csv(data.frame(residual=model$residuals,
                     fitted=model$fitted.values,
                     actual=data$spending),
          row.names=FALSE,
          file=resid_file)

# Assess on validation data
predicted <- predict(model, newdata=as.data.frame(as.matrix(X_validate)), type="response")
actual <- as.vector(y[validate, outcome])
print(paste("auc", auc_cost_capture(predicted, actual)))

# Save predictions
write.csv(data.frame(SAMPLE_ID=y$SAMPLE_ID[validate],
		     predicted=predicted,
		     actual=actual),
          row.names=FALSE,
          file=validate_file)

# Predict on test data
predicted <- predict(model, newdata=as.data.frame(as.matrix(X_test)), type="response")
actual <- as.vector(y[test, outcome])

# Save predictions
write.csv(data.frame(SAMPLE_ID=y$SAMPLE_ID[test],
		     predicted=predicted,
		     actual=actual),
          row.names=FALSE,
          file=test_file)

