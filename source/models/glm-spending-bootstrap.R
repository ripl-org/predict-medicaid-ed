library(assertthat)
library(Matrix)

args <- commandArgs(trailingOnly=TRUE)

auc_cost_capture <- function(predicted, actual) {
    return(mean(cumsum(actual[order(-predicted)]) /
		cumsum(actual[order(-actual)])))
}

set.seed(args[1])

bootstrap     <- strtoi(args[2])
matrix_file   <- args[3]
selected_file <- args[4]
coef_file     <- args[5]

outcome <- c("FUTURE_SPEND")

# Load matrix and selected variables
load(matrix_file)
selected <- read.csv(selected_file, stringsAsFactors=FALSE)$var
print(selected)

X_train <- X_train[,selected]
y_train <- y[train, outcome]

# Generate a bootstrap training sample
set.seed(sample(2147483647, bootstrap+1)[bootstrap+1])
idx <- sample(1:nrow(X_train), nrow(X_train), replace=TRUE)
X_train <- X_train[idx,]
y_train <- y_train[idx]

# Check condition number
k <- kappa(X_train, exact=TRUE)
print(paste("condition number (kappa):", k))
assert_that(k < 1000)

# Fit OLS model to log spending, conditional on cost emergence
emergent <- y_train > 0
data <- as.data.frame(as.matrix(X_train[emergent,]))
data$spending <- y_train[emergent]
model <- lm(spending ~ ., data)
summary(model)

# Save regression coefficients
write.csv(summary(model)$coefficients, file=coef_file)

