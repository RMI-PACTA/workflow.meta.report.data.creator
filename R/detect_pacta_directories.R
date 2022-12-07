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
    # warning(paste("Missing: ", missing_dirs))
    return(FALSE)
  }
  file_contents <- list.files(path, recursive = TRUE)
  has_parameters <- any(grepl(
    x = file_contents,
    # note ^ and $ in patterns. using file.path to not deal wiith
    # different OS issues
    pattern = file.path("^10_Parameter_File", ".*_PortfolioParameters\\.yml$")
  ))
  if (!has_parameters){
    # warning("Missing Parameters File")
    return(FALSE)
  }
  has_portfolio <- any(grepl(
    x = file_contents,
    pattern = file.path("^20_Raw_Inputs", ".*\\.csv$")
  ))
  if (!has_portfolio){
    # warning("Missing Portfolio File")
    return(FALSE)
  }
  return(TRUE)
}
