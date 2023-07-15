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

interrogate_queue <- function(queue_db, table_name = "qqportfolio"){
  con <- DBI::dbConnect(
    RSQLite::SQLite(),
    queue_db
  )
  queue_table <- dplyr::collect(
    dplyr::tbl(con, table_name)
  )
  DBI::dbDisconnect(con)
  messages <- parse_queue_message(queue_table[["message"]])
  messages[["queue_status"]] <- queue_table[["status"]]
  return(messages)
}

foo <- interrogate_queue("/mnt/rawdata2/MFM2023/queue.sqlite")
