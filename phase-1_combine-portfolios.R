# This script is used to prepare meta portfolios from the Constructiva
# initiative Downloads package. Run this script from a directory which contains
# the unzippded package (as `initiative_package_dir`)

# pass the path of the configuration file as an argument to this script
# Rscrip phase-1_combine-portfolios.R path/to/config.yml
logger::log_threshold(Sys.getenv("LOG_LEVEL", "INFO"))
logger::log_info("Reading config.")
cfg <- config::get(file = commandArgs(trailingOnly = TRUE))

logger::log_info("Preparing paths.")
initiative_package_dir <- cfg[["initiative_package_dir"]]
stopifnot(dir.exists(initiative_package_dir))
raw_portfolios_dir <- file.path(initiative_package_dir, "initiativeDownloads")
stopifnot(dir.exists(raw_portfolios_dir))
portfolio_metadata_file <- file.path(
  initiative_package_dir,
  "portfolio_metadata.csv"
)
stopifnot(file.exists(portfolio_metadata_file))
user_metadata_file <- file.path(initiative_package_dir, "user_metadata.csv")
stopifnot(file.exists(user_metadata_file))

output_dir <- cfg[["output_dir"]]
if (!dir.exists(output_dir)) {
  logger::log_debug("Creating output directory.")
  dir.create(output_dir, recursive = TRUE)
}

combined_orgtype_results_output_dir <- file.path(
  output_dir, "combined", "orgtype_level"
)
if (!dir.exists(combined_orgtype_results_output_dir)) {
  logger::log_debug("Creating combined orgtype results directory.")
  dir.create(combined_orgtype_results_output_dir, recursive = TRUE)
}
combined_user_results_output_dir <- file.path(
  output_dir, "combined", "user_level"
)
if (!dir.exists(combined_user_results_output_dir)) {
  logger::log_debug("Creating combined user results directory.")
  dir.create(combined_user_results_output_dir, recursive = TRUE)
}
combined_portfolio_results_output_dir <- file.path(
  output_dir, "combined", "portfolio_level"
)
if (!dir.exists(combined_portfolio_results_output_dir)) {
  logger::log_debug("Creating combined portfolio results directory.")
  dir.create(combined_portfolio_results_output_dir, recursive = TRUE)
}

logger::log_debug("Preparing PACTA directories")
pacta_directories <- c(
  "00_Log_Files",
  "10_Parameter_File",
  "20_Raw_Inputs",
  "30_Processed_Inputs",
  "40_Results",
  "50_Outputs"
)

output_pacta_dirs <- c(
  file.path(combined_orgtype_results_output_dir, pacta_directories),
  file.path(combined_user_results_output_dir, pacta_directories),
  file.path(combined_portfolio_results_output_dir, pacta_directories)
)
for (this_dir in output_pacta_dirs) {
  if (!dir.exists(this_dir)) {
    logger::log_trace("Creating directory: {this_dir}.")
    dir.create(this_dir, recursive = TRUE)
  }
}

logger::log_info("Preparing project settings.")
project_code <- cfg[["project_code"]]
default_language <- cfg[["default_language"]]
project_prefix <- cfg[["project_prefix"]]
holdings_date <- cfg[["holdings_date"]]

logger::log_debug("Preparing ignore information")
csvs_to_ignore <- cfg[["csvs_to_ignore"]]  # if none, this should be c()
if (is.null(csvs_to_ignore)) {
  csvs_to_ignore <- integer()
} else {
  logger::log_debug("Ignoring portfolios: {csvs_to_ignore}")
}
users_to_ignore <- cfg[["users_to_ignore"]]  # if none, this should be c()
if (is.null(users_to_ignore)) {
  users_to_ignore <- integer()
} else {
  logger::log_debug("Ignoring users: {users_to_ignore}")
}


# read in meta data CSVs -------------------------------------------------------

logger::log_info("Reading metadata files.")
portfolios_meta <- readr::read_csv(
  file = portfolio_metadata_file,
  col_types = readr::cols(
    id = readr::col_integer(),
    user_id = readr::col_integer(),
    group_id = readr::col_integer(),
    type = readr::col_factor(),
    name = readr::col_character(),
    submitted = readr::col_logical(),
    parent = readr::col_integer()
  )
)


users_meta <- readr::read_csv(
  file = user_metadata_file,
  col_types = readr::cols(
    id = readr::col_integer(),
    email_canonical = readr::col_character(),
    organization_type.id = readr::col_integer(),
    organization_type.translationKey = readr::col_factor(),
    organization_type.fullTranslationKey = readr::col_skip(),
    organization_type.__initializer__ = readr::col_skip(),
    organization_type.__cloner__ = readr::col_skip(),
    organization_type.__isInitialized__ = readr::col_skip(),
    organization_name = readr::col_character(),
    job_title = readr::col_character(),
    country = readr::col_factor()
  )
)
if ("organization_type.translationKey" %in% names(users_meta)) {
  users_meta <- dplyr::select(
    users_meta,
    id,
    organization_type = organization_type.translationKey
  )
} else {
  users_meta <- dplyr::select(users_meta, id, organization_type)
}


logger::log_info("Preparing portfolio list.")

logger::log_debug("removing child portfolios.")
portfolio_meta <- dplyr::filter(portfolios_meta, is.na(parent))

logger::log_debug("Removing unsubmitted portfolios.")
portfolio_meta <- dplyr::filter(portfolio_meta, submitted)

logger::log_debug("removing portfolios to be ignored.")
portfolio_meta <- dplyr::filter(portfolio_meta, !(id %in% csvs_to_ignore))

logger::log_debug("removing portfolios from users to be ignored.")
portfolio_meta <- dplyr::filter(portfolio_meta, !(user_id %in% users_to_ignore))

logger::log_debug("checking that all portfolio files exist.")
portfolio_meta <- dplyr::mutate(
  portfolio_meta,
  port_path = file.path(raw_portfolios_dir, paste0(id, ".csv"))
) |>
  dplyr::mutate(port_exists = file.exists(port_path))
stopifnot(all(portfolio_meta[["port_exists"]]))

logger::log_info("Adding peer group info to portfolio metadata")
portfolio_meta <- dplyr::left_join(
  portfolio_meta,
  users_meta,
  by = dplyr::join_by(user_id == id)
)
stopifnot(!anyNA(portfolio_meta[["organization_type"]]))

portfolio_csvs <- portfolio_meta[["port_path"]]
# read in all the specs and remove unusable CSVs -------------------------------

logger::log_info("Reading CSV specs.")
specs <- pacta.portfolio.import::get_csv_specs(portfolio_csvs)
saveRDS(specs, file.path(output_dir, paste0(project_prefix, "_csv_specs.rds")))
portfolio_csvs <- specs$filepath


# read in all the CSVs ---------------------------------------------------------

logger::log_info("Reading CSVs.")
data <- pacta.portfolio.import::read_portfolio_csv(portfolio_csvs)


# add meta data to full data and save it ---------------------------------------

logger::log_info("Adding metadata to portfolio data.")
data <-
  data |>
  dplyr::mutate(
    port_id = suppressWarnings(
      as.numeric(tools::file_path_sans_ext(basename(filepath)))
    )
  ) |>
  dplyr::left_join(
    portfolios_meta[, c("id", "user_id")],
    by = dplyr::join_by(port_id == "id")
  ) |>
  dplyr::left_join(
    users_meta[, c("id", "organization_type")],
    by = dplyr::join_by(user_id == "id")
  )

logger::log_debug("Removing rows with missing port_id or user_id.")
data <- data |>
  dplyr::filter(!is.na(port_id)) |>
  dplyr::filter(!is.na(user_id))

logger::log_debug("Saving full portfolio object")
saveRDS(data, file.path(output_dir, paste0(project_prefix, "_full.rds")))


# prepare meta PACTA project ---------------------------------------------------

logger::log_info("Preparing meta PACTA directory")
meta_output_dir <- file.path(output_dir, "meta")
if (!dir.exists(meta_output_dir)) {
  logger::log_debug("Creating meta output directory.")
  dir.create(meta_output_dir, showWarnings = FALSE)
}

for (dir in file.path(meta_output_dir, pacta_directories)) {
  if (!dir.exists(dir)) {
    logger::log_debug("Creating directory: {dir}.")
    dir.create(dir)
  }
}

data |>
  dplyr::mutate(portfolio_name = "Meta Portfolio") |>
  dplyr::mutate(investor_name = "Meta Investor") |>
  dplyr::select(investor_name, portfolio_name, isin, market_value, currency) |>
  dplyr::group_by_all() |>
  dplyr::ungroup(market_value) |>
  dplyr::summarise(
    market_value = sum(market_value, na.rm = TRUE),
    .groups = "drop"
  ) |>
  readr::write_csv(
    file = file.path(
      meta_output_dir, "20_Raw_Inputs",
      paste0(project_prefix, "_meta.csv")
    )
  )

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
yaml::write_yaml(
  config_list,
  file = file.path(
    meta_output_dir, "10_Parameter_File",
    paste0(project_prefix, "_meta", "_PortfolioParameters.yml")
  )
)

# slices for per user_id -------------------------------------------------------

users_output_dir <- file.path(output_dir, "user_id")
if (!dir.exists(users_output_dir)) {
  logger::log_debug("Creating user_id output directory.")
  dir.create(users_output_dir, showWarnings = FALSE)
}

all_user_ids <- unique(data$user_id)

for (user_id in all_user_ids) {
  logger::log_info("Processing user_id: {user_id}.")
  user_data <- data |> dplyr::filter(user_id == .env$user_id)

  investor_name <- encodeString(as.character(unique(user_data$investor_name)))
  if (length(investor_name) > 1) {
    investor_name <- investor_name[[1]]
    user_data <- user_data |>
      dplyr::mutate(investor_name = .env$investor_name)
  }

  user_data <- user_data |>
    dplyr::mutate(portfolio_name = .env$investor_name)

  peer_group <- unique(user_data$organization_type)
  if (length(peer_group) > 1) {
    peer_group <- peer_group[[1]]
  }

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

  user_id_output_dir <- file.path(
    users_output_dir,
    paste0(project_prefix, "_user_", user_id)
  )
  for (dir in file.path(user_id_output_dir, pacta_directories)) {
    if (!dir.exists(dir)) {
      logger::log_debug("Creating directory: {dir}.")
      dir.create(dir, recursive = TRUE)
    }
  }

  yaml::write_yaml(
    config_list,
    file = file.path(
      user_id_output_dir, "10_Parameter_File",
      paste0(project_prefix, "_user_", user_id, "_PortfolioParameters.yml")
    )
  )

  user_data |>
    dplyr::select(
      investor_name,
      portfolio_name,
      isin,
      market_value,
      currency
    ) |>
    dplyr::group_by_all() |>
    dplyr::ungroup(market_value) |>
    dplyr::summarise(
      market_value = sum(market_value, na.rm = TRUE),
      .groups = "drop"
    ) |>
    readr::write_csv(
      file.path(
        user_id_output_dir, "20_Raw_Inputs",
        paste0(project_prefix, "_user_", user_id, ".csv")
      )
    )

}


# slices for per organization_type ---------------------------------------------

orgs_output_dir <- file.path(output_dir, "organization_type")
if (!dir.exists(orgs_output_dir)) {
  logger::log_debug("Creating organization_type output directory.")
  dir.create(orgs_output_dir, showWarnings = FALSE)
}

all_org_types <- unique(data$organization_type)

for (org_type in all_org_types) {
  logger::log_info("Processing organization_type: {org_type}.")
  org_data <-
    data |>
    dplyr::filter(organization_type == .env$org_type) |>
    dplyr::mutate(investor_name = .env$org_type) |>
    dplyr::mutate(portfolio_name = .env$org_type)

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

  org_type_output_dir <- file.path(
    orgs_output_dir,
    paste0(project_prefix, "_org_", org_type)
  )
  for (dir in file.path(org_type_output_dir, pacta_directories)) {
    if (!dir.exists(dir)) {
      logger::log_debug("Creating directory: {dir}.")
      dir.create(dir, recursive = TRUE)
    }
  }

  yaml::write_yaml(
    config_list,
    file = file.path(
      org_type_output_dir, "10_Parameter_File",
      paste0(project_prefix, "_org_", org_type, "_PortfolioParameters.yml")
    )
  )

  org_data |>
    dplyr::select(
      investor_name,
      portfolio_name,
      isin,
      market_value,
      currency
    ) |>
    dplyr::group_by_all() |>
    dplyr::ungroup(market_value) |>
    dplyr::summarise(
      market_value = sum(market_value, na.rm = TRUE),
      .groups = "drop"
    ) |>
    readr::write_csv(
      file = file.path(
        org_type_output_dir, "20_Raw_Inputs",
        paste0(project_prefix, "_org_", org_type, ".csv")
      )
    )

}

# slices for per port_id -------------------------------------------------------

ports_output_dir <- file.path(output_dir, "port_id")
if (!dir.exists(ports_output_dir)) {
  logger::log_debug("Creating port_id output directory.")
  dir.create(ports_output_dir, showWarnings = FALSE)
}

all_port_ids <- unique(data$port_id)

for (port_id in all_port_ids) {
  logger::log_info("Processing port_id: {port_id}.")
  port_data <-
    data |>
    dplyr::filter(port_id == .env$port_id) |>
    dplyr::mutate(portfolio_name = as.character(.env$port_id))

  portfolio_name <- encodeString(as.character(unique(port_data$portfolio_name)))
  if (length(portfolio_name) > 1) {
    portfolio_name <- port_id
  }

  investor_name <- encodeString(as.character(unique(port_data$investor_name)))
  if (length(investor_name) > 1) {
    investor_name <- investor_name[[1]]
  }

  peer_group <- unique(port_data$organization_type)
  if (length(peer_group) > 1) {
    peer_group <- peer_group[[1]]
  }

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

  port_id_output_dir <- file.path(
    ports_output_dir,
    paste0(project_prefix, "_port_", port_id)
  )
  if (!dir.exists(port_id_output_dir)) {
    logger::log_debug("Creating port_id output directory.")
    dir.create(port_id_output_dir, showWarnings = FALSE)
  }
  for (dir in file.path(port_id_output_dir, pacta_directories)) {
    if (!dir.exists(dir)) {
      logger::log_debug("Creating directory: {dir}.")
      dir.create(dir, recursive = TRUE)
    }
  }

  yaml::write_yaml(
    config_list,
    file = file.path(
      port_id_output_dir, "10_Parameter_File",
      paste0(project_prefix, "_port_", port_id, "_PortfolioParameters.yml")
    )
  )

  port_data |>
    dplyr::select(
      investor_name,
      portfolio_name,
      isin,
      market_value,
      currency
    ) |>
    dplyr::mutate(investor_name = .env$investor_name) |>
    dplyr::mutate(portfolio_name = .env$portfolio_name) |>
    dplyr::group_by_all() |>
    dplyr::ungroup(market_value) |>
    dplyr::summarise(
      market_value = sum(market_value, na.rm = TRUE),
      .groups = "drop"
    ) |>
    readr::write_csv(
      file.path(
        port_id_output_dir, "20_Raw_Inputs",
        paste0(project_prefix, "_port_", port_id, ".csv")
      )
    )
}
