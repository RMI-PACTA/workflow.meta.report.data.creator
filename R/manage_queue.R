supplemental_file <- function(
  name = paste(
    as.character(Sys.info()[["nodename"]]),
    as.character(Sys.getpid()),
    sep = "-"
  )
){
  path <- file.path(normalizePath(dirname(config_file)), "supplemental", name)

  if (!dir.exists(dirname(path))) {
    dir.create(dirname(path))
  }

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

write_supplemental <- function(contents) {
  supplemental_file <- supplemental_file()
  write(
    x = contents,
    file = supplemental_file,
    append = TRUE
  )
}

parse_queue_message <- function(message){
  x <- strsplit(message, split = ",", fixed = TRUE)
  m <- lapply(X =x, FUN = matrix, byrow = TRUE, ncol = 6)
  out <- as.data.frame(do.call(rbind, m), stringsAsFactors = FALSE)
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
