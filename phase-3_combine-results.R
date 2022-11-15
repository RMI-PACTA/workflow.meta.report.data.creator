library("tidyverse")
library("config")

cfg <- config::get(file = commandArgs(trailingOnly = TRUE))

output_dir <- cfg$output_dir
combined_portfolio_results_output_dir <- file.path(output_dir, "combined", "portfolio_level")
combined_user_results_output_dir <- file.path(output_dir, "combined", "user_level")
combined_orgtype_results_output_dir <- file.path(output_dir, "combined", "orgtype_level")

data_path <- cfg$data_path
portfolios_path <- file.path(data_path, "portfolios")
portfolios_meta_csv <- file.path(data_path, "portfolios.csv")
users_meta_csv <- file.path(data_path, "users.csv")

project_code <- cfg$project_code
default_language <- cfg$default_language

project_prefix <- cfg$project_prefix
holdings_date <- cfg$holdings_date

users <- read_csv(users_meta_csv) %>%
  select(
    user_id = "id",
    type_id = "organization_type.id",
    type = "organization_type.translationKey"
  ) %>% mutate_all(as.character)

data_filenames <-
  c(
    "Bonds_results_portfolio.rds",
    "Bonds_results_company.rds",
    "Bonds_results_map.rds",
    "Equity_results_portfolio.rds",
    #"Equity_results_company.rds",
    "Equity_results_map.rds"
  )

input_filenames <- c(
    "overview_portfolio.rds",
    "total_portfolio.rds",
    "emissions.rds",
    "audit_file.rds"
  )

all_filenames <- tibble(filename = data_filenames, num_dir = "40_Results") %>%
  bind_rows(tibble(filename = input_filenames, num_dir = "30_Processed_inputs"))

#--combine org-level results--

org_paths <- tibble(path = list.files(file.path(output_dir, "organization_type"), recursive = TRUE, full.names = TRUE)) %>%
bind_rows(tibble(path = list.files(file.path(output_dir, "meta"), recursive = TRUE, full.names = TRUE))) %>%
mutate(
  filename = basename(path),
  filedir = dirname(path)
) %>%
filter(filename %in% all_filenames$filename) %>%
mutate(org = str_extract(string = as.character(filedir), pattern = "(?<=_org_)\\d+|meta")) %>% #lookahead group. https://stackoverflow.com/a/46788230
left_join(distinct(select(users, type_id, type)), by = c("org" = "type_id")) %>%
print()

for (j in seq(1, nrow(all_filenames))){
  this_filetype <- all_filenames[j, ]
  file_paths <- org_paths %>% filter(filename == this_filetype$filename)
  all_results <- NULL
  for (i in seq(1, nrow(file_paths))){
    this_file <- file_paths[i, ]
    message(paste("processing", this_file$filename, "for org", this_file$org, "--", i, "/", nrow(file_paths),  "--", j, "/", nrow(all_filenames)))
    content <- readRDS(this_file$path) %>%
      mutate(
        investor_name = this_file$type,
        portfolio_name = this_file$type
      )
    all_results <- bind_rows(all_results, content)
  }
 saveRDS(all_results, file.path(combined_orgtype_results_output_dir, this_filetype$num_dir, this_filetype$filename))
}

#--combine user-level results--

user_paths <- tibble(path = list.files(file.path(output_dir, "user_id"), recursive = TRUE, full.names = TRUE)) %>%
mutate(
  filename = basename(path),
  filedir = dirname(path)
) %>%
filter(filename %in% all_filenames$filename) %>%
mutate(user = str_extract(string = as.character(filedir), pattern = "(?<=_user_)\\d+")) %>% #lookahead group. https://stackoverflow.com/a/46788230
left_join(distinct(select(users, user_id, type)), by = c("user" = "user_id")) %>%
print()

for (j in seq(1, nrow(all_filenames))){
  this_filetype <- all_filenames[j, ]
  file_paths <- user_paths %>% filter(filename == this_filetype$filename)
  all_results <- NULL
  for (i in seq(1, nrow(file_paths))){
    this_file <- file_paths[i, ]
    message(paste("processing", this_file$filename, "for user", this_file$user, "--", i, "/", nrow(file_paths),  "--", j, "/", nrow(all_filenames)))
    message(paste("  ", nrow(all_results)))
    content <- readRDS(this_file$path) %>%
      mutate(
        investor_name = this_file$type,
        portfolio_name = this_file$user
      )
    all_results <- bind_rows(all_results, content)
  }
  saveRDS(all_results, file.path(combined_user_results_output_dir, this_filetype$num_dir, this_filetype$filename))
}

file.copy(
  from = file.path(combined_orgtype_results_output_dir, "40_Results", "Equity_results_portfolio.rds"),
  to = file.path(output_dir, paste0(project_code, "_peers_equity_results_portfolio.rds"))
)
file.copy(
  from = file.path(combined_orgtype_results_output_dir, "40_Results", "Bonds_results_portfolio.rds"),
  to = file.path(output_dir, paste0(project_code, "_peers_bonds_results_portfolio.rds"))
)
file.copy(
  from = file.path(combined_user_results_output_dir, "40_Results", "Equity_results_portfolio.rds"),
  to = file.path(output_dir, paste0(project_code, "_peers_equity_results_portfolio_ind.rds"))
)
file.copy(
  from = file.path(combined_user_results_output_dir, "40_Results", "Bonds_results_portfolio.rds"),
  to = file.path(output_dir, paste0(project_code, "_peers_bonds_results_portfolio_ind.rds"))
)
