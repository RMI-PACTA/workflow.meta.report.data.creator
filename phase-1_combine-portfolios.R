# load required packages -------------------------------------------------------

suppressPackageStartupMessages({
  require(R.utils, quietly = TRUE, warn.conflicts = FALSE)
  require(tibble, quietly = TRUE)
  require(fs, quietly = TRUE)
  require(cli, quietly = TRUE)
  require(stringi, quietly = TRUE)
  require(wand, quietly = TRUE)
  require(stringr, quietly = TRUE)
  require(pacta.portfolio.import, quietly = TRUE) # must install with # devtools::install_github("RMI-PACTA/pacta.portfolio.import")
  require(pacta.portfolio.analysis, quietly = TRUE) # must install with # devtools::install_github("RMI-PACTA/pacta.portfolio.analysis")

  library(dplyr, warn.conflicts = FALSE)
  library(devtools, quietly = TRUE, warn.conflicts = FALSE)
  library(purrr)
  library(stringr)
  library(fs)
  library(r2dii.utils)  # must install with # devtools::install_github("2DegreesInvesting/r2dii.utils")
  library(readr)
  library(yaml)
  library(here)
})

cfg <- config::get(file = commandArgs(trailingOnly = TRUE))


# manually set certain values and paths ----------------------------------------

# WARNING!!! These filepaths are easy to mess up. You're much better off
# copy-pasting them from your filesystem rather than trying to manually edit
# bits of it. Seriously. Trust me.

output_dir <- cfg$output_dir # this will likely not work on Windows, so change it!
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

bogus_csvs_to_be_ignored <- cfg$bogus_csvs_to_be_ignored  # if none, this should be c()
if (is.null(bogus_csvs_to_be_ignored)) {bogus_csvs_to_be_ignored <- c()}
users_to_be_ignored <- cfg$users_to_be_ignored  # if none, this should be c()
if (is.null(users_to_be_ignored)) {users_to_be_ignored <- c()}


# check paths and directories --------------------------------------------------

dir.create(output_dir, showWarnings = FALSE)
dir.create(combined_portfolio_results_output_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(file.path(combined_portfolio_results_output_dir, "30_Processed_inputs"), showWarnings = FALSE)
dir.create(file.path(combined_portfolio_results_output_dir, "40_Results"), showWarnings = FALSE)
dir.create(combined_user_results_output_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(file.path(combined_user_results_output_dir, "30_Processed_inputs"), showWarnings = FALSE)
dir.create(file.path(combined_user_results_output_dir, "40_Results"), showWarnings = FALSE)
dir.create(combined_orgtype_results_output_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(file.path(combined_orgtype_results_output_dir, "30_Processed_inputs"), showWarnings = FALSE)
dir.create(file.path(combined_orgtype_results_output_dir, "40_Results"), showWarnings = FALSE)

stopifnot(dir.exists(output_dir))
stopifnot(dir.exists(portfolios_path))
stopifnot(file.exists(portfolios_meta_csv))
stopifnot(file.exists(users_meta_csv))
stopifnot(dir.exists(combined_portfolio_results_output_dir))
stopifnot(dir.exists(file.path(combined_portfolio_results_output_dir, "30_Processed_inputs")))
stopifnot(dir.exists(file.path(combined_portfolio_results_output_dir, "40_Results")))
stopifnot(dir.exists(combined_user_results_output_dir))
stopifnot(dir.exists(file.path(combined_user_results_output_dir, "30_Processed_inputs")))
stopifnot(dir.exists(file.path(combined_user_results_output_dir, "40_Results")))
stopifnot(dir.exists(combined_orgtype_results_output_dir))
stopifnot(dir.exists(file.path(combined_orgtype_results_output_dir, "30_Processed_inputs")))
stopifnot(dir.exists(file.path(combined_orgtype_results_output_dir, "40_Results")))


# set needed values

pacta_directories <- c("00_Log_Files", "10_Parameter_File", "20_Raw_Inputs", "30_Processed_Inputs", "40_Results", "50_Outputs")


# prepare a list of all the CSVs to import -------------------------------------

portfolio_csvs <- list.files(portfolios_path, pattern = "[.]csv$", full.names = TRUE)


# read in meta data CSVs -------------------------------------------------------

portfolios_meta <- read_csv(portfolios_meta_csv, show_col_types = FALSE)
users_meta <- read_csv(users_meta_csv, show_col_types = FALSE)

if ("organization_type.id" %in% names(users_meta)) {
  users_meta <- select(users_meta, id, organization_type = organization_type.id)
} else {
  users_meta <- select(users_meta, id, organization_type)
}


# remove child portfolios -------------------------------------------------

child_ids <- portfolios_meta$id[!is.na(portfolios_meta$parent)]
portfolio_csvs <- portfolio_csvs[!tools::file_path_sans_ext(basename(portfolio_csvs)) %in% child_ids]


# remove unsubmitted CSVs ------------------------------------------------------

unsubmitted_ids <- portfolios_meta$id[portfolios_meta$submitted == 0]
portfolio_csvs <- portfolio_csvs[!tools::file_path_sans_ext(basename(portfolio_csvs)) %in% unsubmitted_ids]


# remove bogus CSVs ------------------------------------------------------------

portfolio_csvs <- portfolio_csvs[! tools::file_path_sans_ext(basename(portfolio_csvs)) %in% bogus_csvs_to_be_ignored]


# read in all the specs and remove unusable CSVs -------------------------------

specs <- get_csv_specs(portfolio_csvs)
saveRDS(specs, file.path(output_dir, paste0(project_prefix, "_csv_specs.rds")))
portfolio_csvs <- specs$filepath


# read in all the CSVs ---------------------------------------------------------

data <- read_portfolio_csv(portfolio_csvs)


# add meta data to full data and save it ---------------------------------------

data <-
  data %>%
  mutate(port_id = suppressWarnings(as.numeric(tools::file_path_sans_ext(basename(filepath))))) %>%
  left_join(portfolios_meta[, c("id", "user_id")], by = c(port_id = "id")) %>%
  left_join(users_meta[, c("id", "organization_type")], by = c(user_id = "id"))

data <-
  data %>%
  filter(!is.na(port_id)) %>%
  filter(!is.na(user_id))

# remove users from analysis
data <- data %>%
  filter(!(user_id %in% users_to_be_ignored))

# `write_csv()` sometimes fails on Windows and is not necessary, so commented out until solved
# write_csv(data, file = file.path(output_dir, paste0(project_prefix, "_full.csv")))
saveRDS(data, file.path(output_dir, paste0(project_prefix, "_full.rds")))


# prepare meta PACTA project ---------------------------------------------------

meta_output_dir <- file.path(output_dir, "meta")
dir.create(meta_output_dir, showWarnings = FALSE)

dir_create(file.path(meta_output_dir, pacta_directories))

data %>%
  mutate(portfolio_name = "Meta Portfolio") %>%
  mutate(investor_name = "Meta Investor") %>%
  select(investor_name, portfolio_name, isin, market_value, currency) %>%
  group_by_all() %>% ungroup(market_value) %>%
  summarise(market_value = sum(market_value, na.rm = TRUE), .groups = "drop") %>%
  write_csv(file = file.path(meta_output_dir, "20_Raw_Inputs", paste0(project_prefix, "_meta.csv")))

config_list <-
  list(
    default = list(
      parameters = list(
        portfolio_name = "Meta Portfolio",
        investor_name = "Meta Investor",
        peer_group = paste0(project_prefix, "_meta"),
        language = default_language,
        project_code = project_code,
        holdings_date = holdings_date
      )
    )
  )
write_yaml(config_list, file = file.path(meta_output_dir, "10_Parameter_File", paste0(project_prefix, "_meta", "_PortfolioParameters.yml")))


# slices for per user_id -------------------------------------------------------

users_output_dir <- file.path(output_dir, "user_id")
dir.create(users_output_dir, showWarnings = FALSE)

all_user_ids <- unique(data$user_id)

for (user_id in all_user_ids) {
  user_data <- data %>% dplyr::filter(user_id == .env$user_id)

  investor_name <- encodeString(as.character(unique(user_data$investor_name)))
  if (length(investor_name) > 1) {
    investor_name <- investor_name[[1]]
    user_data <- user_data %>% mutate(investor_name = .env$investor_name)
  }

  user_data <- user_data %>% mutate(portfolio_name = .env$investor_name)

  peer_group <- unique(user_data$organization_type)
  if (length(peer_group) > 1) { peer_group <- peer_group[[1]] }

  config_list <-
    list(
      default = list(
        parameters = list(
          portfolio_name = as.character(investor_name),
          investor_name = as.character(investor_name),
          peer_group = peer_group,
          language = default_language,
          project_code = project_code,
          holdings_date = holdings_date
        )
      )
    )

  user_id_output_dir <- file.path(users_output_dir, paste0(project_prefix, "_user_", user_id))
  dir_create(file.path(user_id_output_dir, pacta_directories))

  write_yaml(config_list, file = file.path(user_id_output_dir, "10_Parameter_File", paste0(project_prefix, "_user_", user_id, "_PortfolioParameters.yml")))

  user_data %>%
    select(investor_name, portfolio_name, isin, market_value, currency) %>%
    group_by_all() %>% ungroup(market_value) %>%
    summarise(market_value = sum(market_value, na.rm = TRUE), .groups = "drop") %>%
    write_csv(file.path(user_id_output_dir, "20_Raw_Inputs", paste0(project_prefix, "_user_", user_id, ".csv")))
}


# slices for per organization_type ---------------------------------------------

orgs_output_dir <- file.path(output_dir, "organization_type")
dir.create(orgs_output_dir, showWarnings = FALSE)

all_org_types <- unique(data$organization_type)

for (org_type in all_org_types) {
  org_data <-
    data %>%
    dplyr::filter(organization_type == .env$org_type) %>%
    mutate(investor_name = .env$org_type) %>%
    mutate(portfolio_name = .env$org_type)

  config_list <-
    list(
      default = list(
        parameters = list(
          portfolio_name = org_type,
          investor_name = org_type,
          peer_group = org_type,
          language = default_language,
          project_code = project_code,
          holdings_date = holdings_date
        )
      )
    )

  org_type_output_dir <- file.path(orgs_output_dir, paste0(project_prefix, "_org_", org_type))
  dir_create(file.path(org_type_output_dir, pacta_directories))

  write_yaml(config_list, file = file.path(org_type_output_dir, "10_Parameter_File", paste0(project_prefix, "_org_", org_type, "_PortfolioParameters.yml")))

  org_data %>%
    select(investor_name, portfolio_name, isin, market_value, currency) %>%
    group_by_all() %>% ungroup(market_value) %>%
    summarise(market_value = sum(market_value, na.rm = TRUE), .groups = "drop") %>%
    write_csv(file.path(org_type_output_dir, "20_Raw_Inputs", paste0(project_prefix, "_org_", org_type, ".csv")))
}

# slices for per port_id -------------------------------------------------------

ports_output_dir <- file.path(output_dir, "port_id")
dir.create(ports_output_dir, showWarnings = FALSE)

all_port_ids <- unique(data$port_id)

for (port_id in all_port_ids) {
  port_data <-
    data %>%
    dplyr::filter(port_id == .env$port_id) %>%
    mutate(portfolio_name = as.character(.env$port_id))

  portfolio_name <- encodeString(as.character(unique(port_data$portfolio_name)))
  if (length(portfolio_name) > 1) { portfolio_name <- port_id }

  investor_name <- encodeString(as.character(unique(port_data$investor_name)))
  if (length(investor_name) > 1) { investor_name <- investor_name[[1]] }

  peer_group <- unique(port_data$organization_type)
  if (length(peer_group) > 1) { peer_group <- peer_group[[1]] }

  config_list <-
    list(
      default = list(
        parameters = list(
          portfolio_name = as.character(portfolio_name),
          investor_name = as.character(investor_name),
          peer_group = peer_group,
          language = default_language,
          project_code = project_code,
          holdings_date = holdings_date
        )
      )
    )

  port_id_output_dir <- file.path(ports_output_dir, paste0(project_prefix, "_port_", port_id))
  dir_create(file.path(port_id_output_dir, pacta_directories))

  write_yaml(config_list, file = file.path(port_id_output_dir, "10_Parameter_File", paste0(project_prefix, "_port_", port_id, "_PortfolioParameters.yml")))

  port_data %>%
    select(investor_name, portfolio_name, isin, market_value, currency) %>%
    mutate(investor_name = .env$investor_name) %>%
    mutate(portfolio_name = .env$portfolio_name) %>%
    group_by_all() %>% ungroup(market_value) %>%
    summarise(market_value = sum(market_value, na.rm = TRUE), .groups = "drop") %>%
    write_csv(file.path(port_id_output_dir, "20_Raw_Inputs", paste0(project_prefix, "_port_", port_id, ".csv")))
}


# slices for per port_id -------------------------------------------------------

ports_output_dir <- file.path(output_dir, "port_id")
dir.create(ports_output_dir, showWarnings = FALSE)

all_port_ids <- unique(data$port_id)

for (port_id in all_port_ids) {
  port_data <-
    data %>%
    dplyr::filter(port_id == .env$port_id) %>%
    mutate(portfolio_name = as.character(.env$port_id))

  portfolio_name <- encodeString(as.character(unique(port_data$portfolio_name)))
  if (length(portfolio_name) > 1) { portfolio_name <- port_id }

  investor_name <- encodeString(as.character(unique(port_data$investor_name)))
  if (length(investor_name) > 1) { investor_name <- investor_name[[1]] }

  peer_group <- unique(port_data$organization_type)
  if (length(peer_group) > 1) { peer_group <- peer_group[[1]] }

  config_list <-
    list(
      default = list(
        parameters = list(
          portfolio_name = as.character(portfolio_name),
          investor_name = as.character(investor_name),
          peer_group = peer_group,
          language = default_language,
          project_code = project_code,
          holdings_date = holdings_date
        )
      )
    )

  port_id_output_dir <- file.path(ports_output_dir, paste0(project_prefix, "_port_", port_id))
  dir_create(file.path(port_id_output_dir, pacta_directories))

  write_yaml(config_list, file = file.path(port_id_output_dir, "10_Parameter_File", paste0(project_prefix, "_port_", port_id, "_PortfolioParameters.yml")))

  port_data %>%
    select(investor_name, portfolio_name, isin, market_value, currency) %>%
    mutate(investor_name = .env$investor_name) %>%
    mutate(portfolio_name = .env$portfolio_name) %>%
    group_by_all() %>% ungroup(market_value) %>%
    summarise(market_value = sum(market_value, na.rm = TRUE), .groups = "drop") %>%
    write_csv(file.path(port_id_output_dir, "20_Raw_Inputs", paste0(project_prefix, "_port_", port_id, ".csv")))
}


# slices for per port_id -------------------------------------------------------

ports_output_dir <- file.path(output_dir, "port_id")
dir.create(ports_output_dir, showWarnings = FALSE)

all_port_ids <- unique(data$port_id)

for (port_id in all_port_ids) {
  port_data <-
    data %>%
    dplyr::filter(port_id == .env$port_id) %>%
    mutate(portfolio_name = as.character(.env$port_id))

  portfolio_name <- encodeString(as.character(unique(port_data$portfolio_name)))
  if (length(portfolio_name) > 1) { portfolio_name <- port_id }

  investor_name <- encodeString(as.character(unique(port_data$investor_name)))
  if (length(investor_name) > 1) { investor_name <- investor_name[[1]] }

  peer_group <- unique(port_data$organization_type)
  if (length(peer_group) > 1) { peer_group <- peer_group[[1]] }

  config_list <-
    list(
      default = list(
        parameters = list(
          portfolio_name = as.character(portfolio_name),
          investor_name = as.character(investor_name),
          peer_group = peer_group,
          language = default_language,
          project_code = project_code,
          holdings_date = holdings_date
        )
      )
    )

  port_id_output_dir <- file.path(ports_output_dir, paste0(project_prefix, "_port_", port_id))
  dir_create(file.path(port_id_output_dir, pacta_directories))

  write_yaml(config_list, file = file.path(port_id_output_dir, "10_Parameter_File", paste0(project_prefix, "_port_", port_id, "_PortfolioParameters.yml")))

  port_data %>%
    select(investor_name, portfolio_name, isin, market_value, currency) %>%
    mutate(investor_name = .env$investor_name) %>%
    mutate(portfolio_name = .env$portfolio_name) %>%
    group_by_all() %>% ungroup(market_value) %>%
    summarise(market_value = sum(market_value, na.rm = TRUE), .groups = "drop") %>%
    write_csv(file.path(port_id_output_dir, "20_Raw_Inputs", paste0(project_prefix, "_port_", port_id, ".csv")))
}


