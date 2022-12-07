require("dplyr")

prepare_queue_message <- function(
  relpath,
  portfolio_name_ref_all,
  status,
  worker = Sys.info()[["nodename"]],
  pid = Sys.getpid(),
  x = NULL
  ) {
  if (is.data.frame(x)) {
    relpath <- x$relpath
    portfolio_name_ref_all <- x$portfolio_name_ref_all
  }
  tibble(
    relpath = relpath,
    portfolio_name_ref_all = portfolio_name_ref_all,
    status = status,
    worker = worker,
    pid = pid,
    timestamp = format(Sys.time(), tz = "UTC")
  )
}

write_queue <- function(contents, queue_file) {
  is_new_file <- !file.exists(queue_file)
  write.table(
    x = contents,
    file = queue_file,
    sep = ",",
    append = TRUE,
    col.names = is_new_file,
    row.names = FALSE
  )
}

get_queue_status <- function(queue_file) {
  read.csv(queue_file) %>%
    group_by(relpath, portfolio_name_ref_all) %>%
    dplyr::filter(timestamp == max(timestamp)) %>%
    ungroup()
}

get_next_queue_item <- function(queue_file, waiting_status = c("waiting")) {
  next_item <- get_queue_status(queue_file) %>%
    dplyr::filter(status %in% waiting_status) %>%
    dplyr::slice(1)
}


get_queue_stats <- function(queue_file, write_message = TRUE) {

  queue <- read.csv(queue_file)

  all_runtimes <- queue %>%
    group_by(relpath, portfolio_name_ref_all, worker, pid) %>%
    dplyr::filter(any(status == "done")) %>%
    mutate(
    queue_time = if_else(
      status %in% c("running", "done"),
      timestamp,
      NULL
      )
  ) %>%
  summarize(
    time_to_run = difftime(max(queue_time), min(queue_time))
  )

  average_runtime <- all_runtimes %>% pull(time_to_run) %>% mean(na.rm = TRUE)

  current_status <- queue %>%
    group_by(relpath, portfolio_name_ref_all) %>%
    dplyr::filter(timestamp == max(timestamp)) %>%
    group_by(status) %>%
    count() %>%
    ungroup()

  outstanding_portfolios <- current_status %>%
    dplyr::filter(status %in% c("done", "waiting")) %>%
    pull(n) %>%
    sum(na.rm = TRUE)

  expected_time_to_finish <- outstanding_portfolios * average_runtime

  if (write_message) {
    message("Queue Status:")
    for (i in seq_along(current_status$status)) {
      message(paste(current_status$status[i], "-", current_status$n[i]))
    }
    message(paste("Average run time:", format(average_runtime)))
    message(paste(
        "Estimated time to finish:",
        format(expected_time_to_finish),
        "(",
        format(Sys.time() + expected_time_to_finish, tz = "UTC"), "UTC",
        ")"
        ))
  }

  return(invisible(list(
        queue = queue,
        all_runtimes = all_runtimes,
        average_runtime = average_runtime,
        current_status = current_status,
        expected_time_to_finish = expected_time_to_finish
        )))
}
