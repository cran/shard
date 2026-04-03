## ----setup, include = FALSE---------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>",
  message = FALSE,
  warning = FALSE
)

## ----load-shard---------------------------------------------------------------
library(shard)

## ----first-example------------------------------------------------------------
set.seed(42)
X <- matrix(rnorm(5000), nrow = 100, ncol = 50)

# Share the matrix (zero-copy for workers)
X_shared <- share(X)

# Allocate an output buffer
out <- buffer("double", dim = ncol(X))

# Define column shards and run
blocks <- shards(ncol(X), workers = 2)
run <- shard_map(
  blocks,
  borrow = list(X = X_shared),
  out    = list(out = out),
  workers = 2,
  fun = function(shard, X, out) {
    for (j in shard$idx) {
      out[j] <- mean(X[, j])
    }
  }
)

# Read results from the buffer
result <- out[]
head(result)

## ----share-input--------------------------------------------------------------
X_shared <- share(X)
is_shared(X_shared)
shared_info(X_shared)

## ----buffer-demo--------------------------------------------------------------
buf <- buffer("double", dim = c(10, 5))
buf[1:5, 1] <- rnorm(5)
buf[6:10, 1] <- rnorm(5)
buf[, 1]

## ----buffer-types-------------------------------------------------------------
int_buf <- buffer("integer", dim = 100)
mat_buf <- buffer("double", dim = c(50, 20))

## ----shards-demo--------------------------------------------------------------
blocks <- shards(1000, workers = 4)
blocks

## ----shard-indices------------------------------------------------------------
blocks[[1]]$idx[1:10]  # first 10 indices of shard 1

## ----shard-map-example--------------------------------------------------------
set.seed(1)
X <- matrix(rnorm(2000), nrow = 100, ncol = 20)
X_shared <- share(X)
col_sds <- buffer("double", dim = ncol(X))

blocks <- shards(ncol(X), workers = 2)
run <- shard_map(
  blocks,
  borrow  = list(X = X_shared),
  out     = list(col_sds = col_sds),
  workers = 2,
  fun = function(shard, X, col_sds) {
    for (j in shard$idx) {
      col_sds[j] <- sd(X[, j])
    }
  }
)

# Results are already in the buffer
sd_values <- col_sds[]

# Verify against base R
all.equal(sd_values, apply(X, 2, sd))

## ----return-values------------------------------------------------------------
blocks <- shards(10, workers = 2)
run <- shard_map(
  blocks,
  workers = 2,
  fun = function(shard) {
    sum(shard$idx)
  }
)
results(run)

## ----apply-matrix-------------------------------------------------------------
set.seed(1)
X <- matrix(rnorm(2000), nrow = 100, ncol = 20)
y <- rnorm(100)

# Correlate each column of X with y
cors <- shard_apply_matrix(
  X,
  MARGIN = 2,
  FUN = function(v, y) cor(v, y),
  VARS = list(y = y),
  workers = 2
)
head(cors)

## ----lapply-shared------------------------------------------------------------
chunks <- lapply(1:10, function(i) rnorm(100))

means <- shard_lapply_shared(
  chunks,
  FUN = mean,
  workers = 2
)
unlist(means)

## ----diagnostics--------------------------------------------------------------
report(result = run)

## ----pool-management, eval = FALSE--------------------------------------------
# # Create a pool with 4 workers and a 1GB memory cap
# pool_create(n = 4, rss_limit = "1GB")
# 
# # Check pool health
# pool_status()
# 
# # Run multiple shard_map() calls (reuses the same pool)
# run1 <- shard_map(shards(1000), workers = 4, fun = function(s) sum(s$idx))
# run2 <- shard_map(shards(500),  workers = 4, fun = function(s) mean(s$idx))
# 
# # Shut down workers when done
# pool_stop()

## ----cow-deny, eval = FALSE---------------------------------------------------
# shard_map(
#   shards(10),
#   borrow = list(X = share(matrix(1:100, 10, 10))),
#   workers = 2,
#   cow = "deny",
#   fun = function(shard, X) {
#     X[1, 1] <- 999  # Error: mutation denied
#   }
# )

## ----cleanup, include = FALSE-------------------------------------------------
try(pool_stop(), silent = TRUE)

## ----cleanup-show, eval = FALSE-----------------------------------------------
# pool_stop()

