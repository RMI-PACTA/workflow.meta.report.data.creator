detect_pacta_dirs <- function(path) {
  all_dirs <- tibble(
    relpath = list.dirs(path, recursive = TRUE, full.names = FALSE)
  ) %>%
  rowwise() %>%
  mutate(
    is_pacta_dir = check_working_dir_structure(file.path(path, relpath))
  )
  return(all_dirs)
}

check_working_dir_structure <- function(path) {
  dir_structure <- list.dirs(path, recursive = TRUE, full.names = FALSE)
  expected_files <- c(
    "10_Parameter_File",
    "20_Raw_Inputs",
    "30_Processed_Inputs",
    "40_Results",
    "50_Outputs"
  )
  missing_dirs <- setdiff(expected_files, dir_structure)
  if (length(missing_dirs > 0)) {
    return(FALSE)
  }
  file_contents <- list.files(path, recursive = TRUE)
  has_parameters <- any(grepl(
    x = file_contents,
    # note ^ and $ in patterns. using file.path to not deal wiith
    # different OS issues
    pattern = file.path("^10_Parameter_File", ".*_PortfolioParameters\\.yml$")
  ))
  if (!has_parameters) {
    return(FALSE)
  }
  has_portfolio <- any(grepl(
    x = file_contents,
    pattern = file.path("^20_Raw_Inputs", ".*\\.csv$")
  ))
  if (!has_portfolio) {
    return(FALSE)
  }
  return(TRUE)
}

get_portfolio_refname <- function(path) {
  files <- list.files(path, recursive = TRUE, full.names = FALSE)
  portfolios <- grep(
    x = files,
    pattern = file.path("^20_Raw_Inputs", ".*\\.csv$"),
    value = TRUE
  )
  candidate_names <- tools::file_path_sans_ext(basename(portfolios))
  refnames <- c()
  for (candidate in candidate_names) {
    has_parameters <- any(
      grepl(
        x = files,
        # note ^ and $ in patterns. using file.path to not deal wiith
        # different OS issues
        pattern = file.path(
          "^10_Parameter_File",
          paste0(candidate, "_PortfolioParameters\\.yml$")
        )
      )
    )
    if (has_parameters) {
      refnames <- c(refnames, candidate)
    } else {
      abs_path <- list.files(
        path,
        recursive = TRUE,
        full.names = TRUE,
        pattern = paste0(candidate, ".csv")
      )
      warning("No parameters file found matching portfolio:", abs_path)
    }
  }
  return(list(refnames))
}

has_pacta_results <- function(path, portfolio_name_ref_all) {
  files <- list.files(path, recursive = TRUE, full.names = FALSE)
  has_results_files <- any(
      grepl(
        x = files,
        pattern = file.path("40_Results", portfolio_name_ref_all)
      )
    )
}