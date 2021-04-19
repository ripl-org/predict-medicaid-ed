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

outcome <- c("COST_EMERGENCE")

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

# Fit logit model
data <- as.data.frame(as.matrix(X_train))
data$emergence <- as.vector(y_train)
model <- glm(emergence ~ ., data=data, family=binomial())
summary(model)

# Convert coefficients to odds ratios and standard errors to 95% C.I.
coef <- summary(model)$coefficients
odds <- exp(coef[,1])
ci_lower <- exp(coef[,1] - 1.96*coef[,2])
ci_upper <- exp(coef[,1] + 1.96*coef[,2])

# Save regression coefficients
write.csv(data.frame(var=rownames(coef),
                     coef=coef[,1],
                     odds=odds,
                     ci_lower=ci_lower,
                     ci_upper=ci_upper,
                     t=coef[,3],
                     p=coef[,4]),
          row.names=FALSE,
          file=coef_file)

