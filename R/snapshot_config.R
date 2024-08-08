snapshot_config <- function(s3_bucket, region) {
  tryCatch({
    credentials <- aws.signature::locate_credentials(default_region = region, verbose = TRUE)
    
    if (is.null(credentials$key) || is.null(credentials$secret)) {
      stop("AWS credentials not found. Please configure your AWS CLI or provide credentials file.")
    }
    
    list(
      s3_bucket = s3_bucket,
      credentials = credentials
    )
  }, error = function(e) {
    stop("Failed to configure AWS credentials: ", e$message)
  })
}
