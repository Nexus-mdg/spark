# DATAFRAME IN-MEMORY CACHE WITH REDIS (CSV FORMAT)
# =================================================

library(redux)  # Install with: install.packages("redux")
library(dplyr)
library(jsonlite)  # Install with: install.packages("jsonlite")

# Create Redis connection
redis_con <- redux::hiredis(
  host = "localhost",
  port = 6379
)

# Helper functions for DataFrame caching
# ======================================

# Store DataFrame in Redis (CSV format for Python compatibility)
cache_dataframe <- function(df, name, description = "") {
  # Convert DataFrame to CSV string
  csv_string <- paste(capture.output(write.csv(df, file = "", row.names = FALSE)), collapse = "\n")

  # Store CSV in Redis
  redis_con$SET(paste0("df:", name), csv_string)

  # Store metadata as JSON for easier Python parsing
  # Convert object.size to numeric to avoid JSON serialization issues
  size_mb <- as.numeric(object.size(df)) / 1024^2

  metadata <- list(
    name = name,
    rows = nrow(df),
    cols = ncol(df),
    columns = names(df),
    description = description,
    timestamp = as.character(Sys.time()),
    size_mb = round(size_mb, 2),
    format = "csv"
  )

  # Convert metadata to JSON
  metadata_json <- jsonlite::toJSON(metadata, auto_unbox = TRUE)
  redis_con$SET(paste0("meta:", name), metadata_json)

  # Add to index
  redis_con$SADD("dataframe_index", name)

  cat("💾 Cached DataFrame:", name, "\n")
  cat("📊 Size:", nrow(df), "rows x", ncol(df), "columns\n")
  cat("💿 Memory:", round(size_mb, 2), "MB\n")
  cat("✅ Available in Redis cache (CSV format)\n\n")
}

# Load DataFrame from Redis (CSV format)
load_cached_dataframe <- function(name) {
  # Check if exists
  if (!redis_con$EXISTS(paste0("df:", name))) {
    stop(paste("DataFrame", name, "not found in cache!"))
  }

  # Load CSV string and convert back to DataFrame
  csv_string <- redis_con$GET(paste0("df:", name))
  df <- read.csv(text = csv_string, stringsAsFactors = FALSE)

  # Load metadata
  meta_json <- redis_con$GET(paste0("meta:", name))
  metadata <- jsonlite::fromJSON(meta_json)

  cat("📂 Loaded from cache:", name, "\n")
  cat("📊 Size:", nrow(df), "rows x", ncol(df), "columns\n")
  cat("⚡ Instant loading from RAM!\n\n")

  return(df)
}
