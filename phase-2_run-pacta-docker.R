library("dplyr")
library("tibble")
library("purrr")
library("tidyr")
library("fs")
library("here")
library("config")


cfg <- config::get(file = commandArgs(trailingOnly = TRUE))

output_dir <- cfg$output_dir # this will likely not work on Windows, so change it!
project_prefix <- cfg$project_prefix
group_types <- cfg$group_types

pacta_directories <- c("00_Log_Files", "10_Parameter_File", "20_Raw_Inputs", "30_Processed_Inputs", "40_Results", "50_Outputs")

if (is.null(group_types)){
  group_types <- c(
    "meta",
    "organization_type",
    "user_id" # ,
    # "port_id"
  )
}

all_paths <- tibble(type = setdiff(group_types, "meta")) %>% #so the meta 10/20/30 dirs don't get expanded
  mutate(path = purrr::map(type, ~ list.dirs(file.path(output_dir, .x), recursive = FALSE))) %>%
  tidyr::unnest(path) %>%
  bind_rows(tibble(type = "meta", path = file.path(output_dir, "meta"))) %>%
  mutate(portfolio_name_ref_all = case_when(
    type == "meta" ~ paste0(project_prefix, "_meta"),
    TRUE ~ basename(path)
  )) %>%
  mutate(type = factor(type, ordered = TRUE, levels = group_types)) %>%
  arrange(type, portfolio_name_ref_all) %>%
  filter(!portfolio_name_ref_all %in% c(
    ""
  ))

script_path <- here("transitionmonitor_docker", "run-like-constructiva-flags.sh")
working_dir <- here("working_dir")
user_dir <- here("user_results")
dir_create(file.path(user_dir, "4"))
stopifnot(file.exists(script_path))
stopifnot(dir.exists(user_dir))

for ( i in seq(1, nrow(all_paths)) ){
  this_row <- all_paths[i, ]
  message(paste(Sys.time(), this_row$type, this_row$portfolio_name_ref_all, "--", i, "/", nrow(all_paths)))
  message(paste("  ", this_row$path))
  these_dirs <- file.path(this_row$path, pacta_directories)
  stopifnot(all(dir.exists(these_dirs)))
  has_results <- (length(list.files(file.path(this_row$path, "40_Results"))) > 0)
  if (has_results){
    message("  Results already exist, skipping")
  } else {
    message("  Running PACTA")
    if (dir.exists(working_dir)) {
      dir_delete(working_dir)
    }
    dir_create(working_dir)
    portfolio_name_ref_all <- this_row$portfolio_name_ref_all
    dir_copy(this_row$path, file.path(working_dir, portfolio_name_ref_all), overwrite = TRUE)
    tic <- Sys.time()
    system2(
      command = script_path,
      args = c(
        paste0("-p ", "\"", this_row$portfolio_name_ref_all, "\""),
	paste("-w", working_dir),
	paste("-y", user_dir),
	"-r /bound/bin/run-r-scripts-results-only"
      )
    )
    message(paste("  ", format.difftime(Sys.time() - tic)))
    dir_delete(this_row$path)
    dir_copy(file.path(working_dir, portfolio_name_ref_all), this_row$path, overwrite = TRUE)
  }
}
