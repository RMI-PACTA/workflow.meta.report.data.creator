queue_lock_file <- function(queue){
  path <- file.path(normalizePath(queue$path()), "lock")
  if (!file.exists(path)) {
    file.create(path)
  }
  return(path)
}

supplemental_file <- function(queue){
  path <- file.path(normalizePath(queue$path()), "supplemental")
  if (!file.exists(path)) {
    file.create(path)
  }
  return(path)
}

prepare_queue_message <- function(
  relpath,
  portfolio_name_ref_all,
  status,
  worker = Sys.info()[["nodename"]],
  pid = Sys.getpid()
  ) {
  paste(
    relpath = as.character(relpath),
    portfolio_name_ref_all = as.character(portfolio_name_ref_all),
    status = as.character(status),
    worker = as.character(worker),
    pid = as.character(pid),
    timestamp = as.character(
        format(Sys.time(), format = "%Y-%m-%d %H:%M:%OS6", tz = "UTC")
        ),
    sep = ","
  )
}

write_supplemental <- function(contents, queue) {
  supplemental_file <- supplemental_file(queue)
  on.exit(filelock::unlock(lock))
  lock <- filelock::lock(queue_lock_file(queue))
  write(
    x = contents,
    file = supplemental_file,
    append = TRUE
  )
}

parse_queue_message <- function(message){
  x <- strsplit(message, split = ",", fixed = TRUE)
  m <- lapply(X =x, FUN = matrix, byrow = TRUE, ncol = 6)
  out <- as.data.frame(do.call(rbind, m))
  colnames(out) <- c(
    "relpath",
    "portfolio_name_ref_all",
    "status",
    "worker",
    "pid",
    "timestamp"
  )
  return(out)
}

read_supplemental <- function(queue, skip = 0, n = Inf){
  nmax = ifelse(identical(n, Inf), -1, n)
  lock <- filelock::lock(queue_lock_file(queue))
  raw_contents <- scan(
    file = supplemental_file(queue),
    skip = skip,
    what = character(),
    nmax = nmax,
    # using a sep that shouldn't appear in the contents
    sep = "|",
    na.strings = NULL,
    quiet = TRUE
  )
  filelock::unlock(lock)
  out <- parse_queue_message(raw_contents)
  return(out)
}

find_runtime <- function(data){
  relpath <- unique(data$relpath)
  portfolio_name_ref_all <- unique(data$portfolio_name_ref_all)
  start_index <- which(data$status == "running")
  done_index <- which(data$status == "done")
  if (!identical(done_index, integer())){
    start_time <- min(data$timestamp[start_index], na.rm = TRUE)
    done_time <- max(data$timestamp[done_index], na.rm = TRUE)
    runtime <- as.numeric(done_time - start_time)
  } else {
    runtime <- NA_integer_
  }
  out <- data.frame(
    relpath = relpath,
    portfolio_name_ref_all = portfolio_name_ref_all,
    runtime = as.numeric(runtime)
  )
  return(out)
}

current_runners <- function(data){
  registered <- data[data$status == "register", c("worker", "pid")]
  deregistered <- data[data$status == "deregister", c("worker", "pid")]
  out <- setdiff(registered, deregistered)
  return(out)
}

get_queue_stats <- function(queue){
  waiting <- queue$count()
  supplemental <- read_supplemental(queue, skip = 0, n = Inf)
  supplemental$timestamp <- as.POSIXct(supplemental$timestamp)
  runtimes <- do.call(rbind, by(
    data = supplemental,
    INDICES = list(supplemental$relpath, supplemental$portfolio_name_ref_all),
    FUN = find_runtime,
    simplify = FALSE
    ))
  avg_runtime <- mean(runtimes$runtime, na.rm = TRUE)
  runners <- current_runners(supplemental)
  num_runners <- nrow(runners)
  browser()
}
