library("tibble")
library("here")
library("txtq")

source(here("R", "manage_queue.R"))
source(here("R", "detect_pacta_directories.R"))

config_file <- commandArgs(trailingOnly = TRUE)
cfg <- config::get(file = config_file)
queue <- txtq(file.path(normalizePath(dirname(config_file)), "queue"))

if (is.null(cfg$output_dir)) {
  cfg$output_dir <- normalizePath(dirname(config_file))
}

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


# This code isn't stable enough yet (and dependent on particular file
# structure) to be run non-interactively
if (FALSE) {

  library("dplyr")
  library("fs")
  library("stringr")

  top_dirs <- list.dirs(
    path = dirname(normalizePath(config_file)),
    recursive = FALSE,
    full.names = TRUE
  )
  port_dirs <- list.dirs(
    path = top_dirs,
    recursive = FALSE,
    full.names = TRUE
  )

  p <- port_dirs %>%
    tibble(full_path = .) %>%
    mutate(
      bn = basename(full_path)
      ) %>%
    mutate(
      relpath = path_rel(
        path = full_path,
        start = dirname(normalizePath(config_file))
      )
    ) %>%
    mutate(
      has_comma = str_detect(string = relpath, pattern = ","),
      has_dot = str_detect(string = relpath, pattern = "\\."),
      has_end_dot = str_detect(string = relpath, pattern = "\\.$")
    )


  portfolios_include <- p %>%
    filter(!has_comma) %>%
    filter(!has_dot)

  portfolios_skip <- filter(p, !(full_path %in% portfolios_include$full_path))
  if (nrow(portfolios_skip) > 0) {warning("Some portfolios have commas in their path. not including")}


  prepare_queue_message(
    relpath = portfolios_include$relpath,
    portfolio_name_ref_all = portfolios_include$bn,
    status = "waiting"
    ) %>%
  queue$push(message = ., title = "waiting")

}

#register_runner
write_supplemental(
  prepare_queue_message(NA, NA, "register"),
  queue
)

this_portfolio <- parse_queue_message(queue$pop()$message)
while (nrow(this_portfolio) == 1) {

  write_supplemental(
    contents = prepare_queue_message(
      relpath = this_portfolio$relpath,
      portfolio_name_ref_all = this_portfolio$portfolio_name_ref_all,
      status = "running"
      ),
    queue = queue
  )

  working_dir <- tempdir()
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

  if (cfg$run_results && cfg$run_reports){
    script_to_run <- "/bound/bin/run-r-scripts"
  } else if (cfg$run_results && !cfg$run_reports){
    script_to_run <- "/bound/bin/run-r-scripts-results-only"
  } else if (!cfg$run_results && cfg$run_reports){
    script_to_run <- "/bound/bin/run-r-scripts-outputs-only"
  }

  docker_run_args <- c(
    "--rm",
    "--network none",
    "--user 1000:1000",
    "--memory-swappiness=0"
  )

  docker_mount_paths <- c(
    paste0(
      "--mount type=bind,source=",
      shQuote(file.path(working_dir, basename(this_portfolio$relpath))),
      ",", "target='/bound/working_dir'"
      ),
    paste0(
      "--mount type=bind,source=",
      shQuote(file.path(user_dir)),
      ",", "target='/user_results'"
    )
  )

  docker_args <- c(
    "run",
    docker_run_args,
    docker_mount_paths,
    paste0(cfg$docker_image, ":", cfg$docker_tag),
    script_to_run,
    shQuote(this_portfolio$portfolio_name_ref_all)
  )

  exit_code <- system2(
    command = "docker",
    args = docker_args
  )

  # This is outside of "if docker exits cleanly" so that we can inspect
  # any records of failure (for example, if there are no PACTA relevant
  # holdings)
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


  if (exit_code == 0L) {

    exit_status <- "done"

    if (!dir.exists(working_dir)) {
      unlink(working_dir, recursive = TRUE)
    }
  } else {
    exit_status <- paste("Failed (", exit_code, ")")
  }

  write_supplemental(
    contents = prepare_queue_message(
      relpath = this_portfolio$relpath,
      portfolio_name_ref_all = this_portfolio$portfolio_name_ref_all,
      status = exit_status
      ),
    queue = queue
  )

  #actually get the next item
  this_portfolio <- parse_queue_message(queue$pop()$message)
}

#deregister_runner
write_supplemental(
  prepare_queue_message(NA, NA, "deregister"),
  queue
)

