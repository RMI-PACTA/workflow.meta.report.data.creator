prepare_queue_message <- function(
  item,
  status,
  worker = Sys.info()[["nodename"]]
  ) {
  tibble(
    item = item,
    status = status,
    worker = worker,
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
    group_by(item) %>%
    dplyr::filter(timestamp == max(timestamp))
}

get_next_queue_item <- function(queue_file) {
  get_queue_status(queue_file) %>%
    dplyr::filter(status == "waiting") %>%
    .[1, "item"]
}
