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

# List cached DataFrames (updated for JSON metadata)
list_cached_dataframes <- function() {
  names <- redis_con$SMEMBERS("dataframe_index")

  if (length(names) == 0) {
    cat("📚 No DataFrames cached yet.\n")
    return(NULL)
  }

  cat("📚 Cached DataFrames:\n")

  for (name in names) {
    if (redis_con$EXISTS(paste0("meta:", name))) {
      meta_json <- redis_con$GET(paste0("meta:", name))
      metadata <- jsonlite::fromJSON(meta_json)

      cat(sprintf("  📄 %s - %d rows x %d cols (%.1f MB) - %s\n",
                  name,
                  metadata$rows,
                  metadata$cols,
                  metadata$size_mb,
                  metadata$description))
    }
  }

  return(names)
}

# Remove DataFrame from cache
remove_cached_dataframe <- function(name) {
  redis_con$DEL(paste0("df:", name))
  redis_con$DEL(paste0("meta:", name))
  redis_con$SREM("dataframe_index", name)
  cat("🗑️  Removed", name, "from cache\n")
}

# Clear all cached DataFrames
clear_cache <- function() {
  names <- redis_con$SMEMBERS("dataframe_index")
  for (name in names) {
    redis_con$DEL(paste0("df:", name))
    redis_con$DEL(paste0("meta:", name))
  }
  redis_con$DEL("dataframe_index")
  cat("🧹 Cleared all cached DataFrames\n")
}

# Check cache statistics (updated for JSON metadata)
cache_stats <- function() {
  names <- redis_con$SMEMBERS("dataframe_index")

  if (length(names) == 0) {
    cat("📊 Cache is empty\n")
    return()
  }

  total_size <- 0
  for (name in names) {
    if (redis_con$EXISTS(paste0("meta:", name))) {
      meta_json <- redis_con$GET(paste0("meta:", name))
      metadata <- jsonlite::fromJSON(meta_json)
      total_size <- total_size + metadata$size_mb
    }
  }

  cat("📊 Cache Statistics:\n")
  cat("   DataFrames:", length(names), "\n")
  cat("   Total Size:", round(total_size, 2), "MB\n")
}

# call trigger_test.sh script with two DataFrame names
# This function assumes you have a script named trigger_test.sh that takes two DataFrame names as arguments.
# The script should be executable and located in the same directory as this R script.
# Example usage: trigger_test("df1", "df2")
trigger_test <- function(df1_name, df2_name) {
  if (missing(df1_name) || missing(df2_name)) {
    stop("❌ Error: Please provide two DataFrame names")
  }

  # Execute the script with parameters
  command <- paste("/home/toavina/Fjelltopp/tan-data-purifier/tan-ibcm-analysis/tests/trigger_test.sh", df1_name, df2_name)
  result <- system(command, intern = TRUE)

  # Print output if any
  if (length(result) > 0) {
    cat(paste(result, collapse = "\n"), "\n")
  }

  invisible(result)
}