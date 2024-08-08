#' Lineage Configuration
#'
#' @param s3_bucket S3 bucket name
#' @param region AWS region (e.g., "eu-west-1")
#' @return A list containing the S3 bucket name, region, and AWS credentials
#' @export
lineage_config <- function(s3_bucket, region) {
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

#' Generate a unique run ID
#'
#' @return A string containing a unique UUID
#' @export
generate_run_id <- function() {
  uuid::UUIDgenerate()
}


#' Create a new lineage session
#'
#' @param namespace Namespace for organizing lineage
#' @param config Configuration returned by lineage_config
#' @return A list containing the session information
#' @export
create_lineage_session <- function(namespace, config) {
  list(
    run_id = generate_run_id(),
    namespace = namespace,
    s3_bucket = config$s3_bucket,
    credentials = config$credentials,
    inputs = list(),
    outputs = list()
  )
}

#' Check if a path is an S3 URI
#'
#' @param path A string representing a file path or S3 URI
#' @return Boolean indicating whether the path is an S3 URI
is_s3_uri <- function(path) {
  grepl("^s3://", path)
}

#' Parse S3 URI into bucket and key
#'
#' @param s3_uri A string representing an S3 URI
#' @return A list containing bucket and key
parse_s3_uri <- function(s3_uri) {
  parts <- strsplit(gsub("^s3://", "", s3_uri), "/")[[1]]
  list(
    bucket = parts[1],
    key = paste(parts[-1], collapse = "/")
  )
}

#' Register a file (input or output)
#'
#' @param session The lineage session object
#' @param file_path Path to the file (local or S3 URI)
#' @param file_type Either "inputs" or "outputs"
#' @return An updated session object
#' @export
register_file <- function(session, file_path, file_type) {
  if (is.null(session$s3_bucket)) stop("S3 bucket not configured.")

  if (is_s3_uri(file_path)) {
    # File is already in S3
    s3_info <- parse_s3_uri(file_path)
    s3_bucket <- s3_info$bucket
    s3_key <- s3_info$key
  } else {
    # Local file, need to upload to S3
    file_name <- basename(file_path)
    s3_key <- paste(session$namespace, session$run_id, file_type, file_name, sep = "/")
    s3_bucket <- session$s3_bucket
    
    aws.s3::put_object(
      file = file_path, 
      object = s3_key, 
      bucket = s3_bucket,
      region = session$credentials$region
    )
  }
  
  s3_obj <- aws.s3::head_object(
    object = s3_key, 
    bucket = s3_bucket, 
    region = session$credentials$region
  )
  
  metadata <- list(
    s3_location = paste0("s3://", s3_bucket, "/", s3_key),
    etag = attr(s3_obj, "etag"),
    last_modified = attr(s3_obj, "last-modified"),
    content_type = attr(s3_obj, "content-type"),
    content_length = attr(s3_obj, "content-length")
  )
  
  session[[file_type]][[file_path]] <- metadata
  session
}

#' Register an input file
#'
#' @param session The lineage session object
#' @param input_path Path to the input file
#' @return An updated session object
#' @export
register_input <- function(session, input_path) {
  register_file(session, input_path, "inputs")
}

#' Register an output file
#'
#' @param session The lineage session object
#' @param output_path Path to the output file
#' @return An updated session object
#' @export
register_output <- function(session, output_path) {
  register_file(session, output_path, "outputs")
}

#' Register all files in a directory (input or output)
#'
#' @param session The lineage session object
#' @param dir_path Path to the directory (local) or S3 URI
#' @param file_type Either "inputs" or "outputs"
#' @param recursive Whether to recursively traverse subdirectories (default: TRUE)
#' @return An updated session object
#' @export
register_directory <- function(session, dir_path, file_type, recursive = TRUE) {
  if (is_s3_uri(dir_path)) {
    # S3 directory
    s3_info <- parse_s3_uri(dir_path)
    objects <- aws.s3::get_bucket(
      bucket = s3_info$bucket,
      prefix = s3_info$key,
      region = session$credentials$region
    )
    
    for (obj in objects) {
      if (!endsWith(obj$Key, "/")) {  # Skip directory objects
        s3_path <- paste0("s3://", s3_info$bucket, "/", obj$Key)
        session <- register_file(session, s3_path, file_type)
      }
    }
  } else {
    # Local directory
    if (!dir.exists(dir_path)) {
      stop(paste("Directory does not exist:", dir_path))
    }
    
    files <- list.files(dir_path, full.names = TRUE, recursive = recursive)
    
    for (file_path in files) {
      if (!file.info(file_path)$isdir) {
        session <- register_file(session, file_path, file_type)
      }
    }
  }
  
  session
}

#' Register an input directory
#'
#' @param session The lineage session object
#' @param input_dir_path Path to the input directory
#' @param recursive Whether to recursively traverse subdirectories (default: TRUE)
#' @return An updated session object
#' @export
register_input_directory <- function(session, input_dir_path, recursive = TRUE) {
  register_directory(session, input_dir_path, "inputs", recursive)
}

#' Register an output directory
#'
#' @param session The lineage session object
#' @param output_dir_path Path to the output directory
#' @param recursive Whether to recursively traverse subdirectories (default: TRUE)
#' @return An updated session object
#' @export
register_output_directory <- function(session, output_dir_path, recursive = TRUE) {
  register_directory(session, output_dir_path, "outputs", recursive)
}

#' Finalize the lineage session
#'
#' @param session The lineage session object
#' @return The unique run ID for this lineage process
#' @export
finalize_lineage <- function(session) {
  if (is.null(session$s3_bucket)) stop("S3 bucket not configured.")

  run_metadata <- list(
    run_id = session$run_id,
    namespace = session$namespace,
    timestamp = Sys.time(),
    inputs = session$inputs,
    outputs = session$outputs
  )
  
  run_metadata_key <- paste(session$namespace, session$run_id, "run_metadata.json", sep = "/")
  aws.s3::put_object(
    file = charToRaw(jsonlite::toJSON(run_metadata, auto_unbox = TRUE)), 
    object = run_metadata_key, 
    bucket = session$s3_bucket,
    region=session$credentials$region
  )
  
  session$run_id
}