library("dplyr")
library("tibble")
library("tidyr")
library("here")

source(here("R", "manage_queue.R"))
source(here("R", "detect_pacta_directories.R"))

cfg <- config::get(file = commandArgs(trailingOnly = TRUE))

if (is.null(cfg$docker_image)) {
  cfg$docker_image <- "transitionmonitordockerregistry.azurecr.io/rmi_pacta"
}

if (is.null(cfg$docker_tag)) {
  cfg$docker_tag <- "latest"
}

if (is.null(cfg$run_results)) {
  cfg$run_results <- TRUE
}

if (is.null(cfg$run_reports)) {
  cfg$run_reports <- TRUE
}



if (!file.exists(cfg$queue_file)) {

  message(paste("Creating queue file:", cfg$queue_file))

  all_paths <- detect_pacta_dirs(cfg$output_dir) %>%
    dplyr::filter(is_pacta_dir) %>%
    mutate(
      portfolio_name_ref_all = get_portfolio_refname(
        file.path(cfg$output_dir, relpath)
      )
      ) %>%
    unnest(portfolio_name_ref_all) %>%
    rowwise() %>%
    mutate(
      has_pacta_results = has_pacta_results(
        path = file.path(cfg$output_dir, relpath),
        portfolio_name_ref_all = portfolio_name_ref_all,
        detect_results = (
          !coalesce(cfg$force_results, FALSE) && cfg$run_results
          ),
        detect_outputs = (
          !coalesce(cfg$force_reports, FALSE) && cfg$run_reports
          )
        )
      ) %>%
    mutate(status = if_else(has_pacta_results, "done", "waiting"))

  prepare_queue_message(
    relpath = all_paths$relpath,
    portfolio_name_ref_all = all_paths$portfolio_name_ref_all,
    status = all_paths$status
    ) %>% write_queue(queue_file = cfg$queue_file)
  message(paste("Queue File written", cfg$queue_file))

}

this_portfolio <- get_next_queue_item(cfg$queue_file)
while (nrow(this_portfolio) == 1) {

  write_queue(
    prepare_queue_message(x = this_portfolio, status = "running"),
    cfg$queue_file
  )

  get_queue_stats(cfg$queue_file)

  working_dir <- tempdir(check = TRUE)
  message(paste("Processing portfolio", this_portfolio$portfolio_name_ref_all))
  message(paste("From directory", this_portfolio$relpath))
  message(paste("In working directory", working_dir))

  user_id <- 4L
  user_dir <- file.path(working_dir, "user_results")
  if (!dir.exists(user_dir)) {
    dir.create(file.path(user_dir, user_id), recursive = TRUE)
  }
  stopifnot(dir.exists(user_dir))

  # paths are tricky with base::file.copy, but needed because
  # fs::dir_copy doesn't allow for ignoring permissions or timestamps
  # (needed on some remote file shares, such as Azure File Share)
  base::file.copy(
    from = file.path(cfg$output_dir, this_portfolio$relpath),
    to = working_dir,
    recursive = TRUE,
    overwrite = TRUE,
    copy.mode = FALSE,
    copy.date = FALSE
  )

  docker_args <- c(
    "--rm",
    "--network none",
    "--user 1000:1000",
    "--memory-swappiness=0",
    paste0(
      "--mount type=bind,source=",
      file.path(working_dir, basename(this_portfolio$relpath)),
      ",", "target=/bound/working_dir"
    ),
    paste0(
      "--mount type=bind,source=",
      file.path(user_dir),
      ",", "target=/user_results"
    )
  )

  script_to_run <- paste0(
    "/bound/bin/",
    dplyr::case_when(
      cfg$run_results && cfg$run_reports ~ "run-r-scripts",
      cfg$run_results && !cfg$run_reports ~ "run-r-scripts-results-only",
      !cfg$run_results && cfg$run_reports ~ "run-r-scripts-outputs-only",
    )
  )


  exit_code <- system2(
    command = "docker",
    args = c(
      "run",
      docker_args,
      paste0(cfg$docker_image, ":", cfg$docker_tag),
      script_to_run,
      this_portfolio$portfolio_name_ref_all
    )
  )

  if (exit_code == 0L) {

    # Note not deleting original copy, but overwriting (only if sucessful)
    # see note above regarding base::file.copy
    base::file.copy(
      from = file.path(working_dir, basename(this_portfolio$relpath)),
      to = file.path(cfg$output_dir, dirname(this_portfolio$relpath)),
      recursive = TRUE,
      overwrite = TRUE,
      copy.mode = FALSE,
      copy.date = FALSE
    )

    exit_status <- "done"

    if (!dir.exists(working_dir)) {
      unlink(working_dir, recursive = TRUE)
    }
  } else {
    exit_status <- paste("Failed (", exit_code, ")")
  }

  write_queue(
    prepare_queue_message(x = this_portfolio, status = exit_status),
    cfg$queue_file
  )
  #actually get the next item
  this_portfolio <- get_next_queue_item(cfg$queue_file)
}
