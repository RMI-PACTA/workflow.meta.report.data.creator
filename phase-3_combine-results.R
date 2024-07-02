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
combined_output_dir <- file.path(output_dir, "combined")

logger::log_info("Preparing project settings.")
project_code <- cfg[["project_code"]]
project_prefix <- cfg[["project_prefix"]]

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

logger::log_info("Adding peer group info to portfolio metadata")
portfolio_meta <- dplyr::left_join(
  portfolio_meta,
  users_meta,
  by = dplyr::join_by(user_id == id)
)
stopifnot(!anyNA(portfolio_meta[["organization_type"]]))

expected_portfolios <- NULL

if ("meta" %in% cfg[["run_categories"]]) {
  logger::log_info("Adding meta portfolios to expected portfolios.")
  expected_portfolios <- dplyr::bind_rows(
    expected_portfolios,
    data.frame(
      category = "meta",
      path = file.path(output_dir, "meta"),
      investor_name = "meta",
      portfolio_name = "meta",
      name = paste0(project_prefix, "_meta"),
      stringsAsFactors = FALSE
    )
  )
}

if ("user_id" %in% cfg[["run_categories"]]) {
  all_user_ids <- unique(portfolio_meta$user_id)
  for (user_id in all_user_ids) {
    logger::log_info("Adding user {user_id} portfolios to expected portfolios.")
    organization_type <- dplyr::filter(
      users_meta,
      id == user_id
    ) |>
      dplyr::pull(organization_type)
    expected_portfolios <- dplyr::bind_rows(
      expected_portfolios,
      data.frame(
        category = "user_id",
        path = file.path(
          output_dir,
          "user_id",
          paste0(project_prefix, "_user_", user_id)
        ),
        investor_name = organization_type,
        portfolio_name = as.character(user_id),
        name = paste0(project_prefix, "_user_", user_id),
        stringsAsFactors = FALSE
      )
    )
  }
}

if ("organization_type" %in% cfg[["run_categories"]]) {
  all_organization_types <- unique(portfolio_meta$organization_type)
  for (organization_type in all_organization_types) {
    logger::log_info(
      "Adding org type {organization_type} portfolios to expected portfolios."
    )
    expected_portfolios <- dplyr::bind_rows(
      expected_portfolios,
      data.frame(
        category = "organization_type",
        path = file.path(
          output_dir,
          "organization_type",
          paste0(project_prefix, "_org_", organization_type)
        ),
        investor_name = organization_type,
        portfolio_name = organization_type,
        name = paste0(project_prefix, "_org_", organization_type),
        stringsAsFactors = FALSE
      )
    )
  }
}

results_filenames <- c(
  # "Bonds_results_company.rds",
  # "Bonds_results_map.rds",
  #"Equity_results_company.rds",
  # "Equity_results_map.rds",
  "Equity_results_portfolio.rds",
  "Bonds_results_portfolio.rds"
)
input_filenames <- c(
  # "overview_portfolio.rds",
  # "total_portfolio.rds",
  # "emissions.rds",
  # "audit_file.rds"
)
filenames_df <- data.frame(
  filename = c(results_filenames, input_filenames),
  num_dir = c(
    rep("40_Results", length.out = length(results_filenames)),
    rep("30_Processed_inputs", length.out = length(input_filenames))
  ),
  stringsAsFactors = FALSE
)

expected_files <- dplyr::cross_join(
  expected_portfolios,
  filenames_df
) |>
  dplyr::mutate(
    filepath = file.path(path, num_dir, name, filename)
  )

logger::log_info("preparing storage endpoint")
storage_endpoint <- AzureStor::storage_endpoint(
  paste0(
    "https://",
    cfg[["storage_account_name"]],
    ".blob.core.windows.net"
  ),
  sas = Sys.getenv("STORAGE_ACCOUNT_SAS")
)
container <- AzureStor::blob_container(storage_endpoint, tolower(project_code))

all_blobs <- AzureStor::list_blobs(container, info = "name")

logger::log_info("Checking for missing files.")
expected_files <- expected_files |>
  dplyr::mutate(
    exists = filepath %in% all_blobs
  )
missing_files <- expected_files |>
  dplyr::filter(!exists)

logger::log_warn("The following files are missing:")
logger::log_warn("{missing_files$filepath}")

logger::log_info("Combining Meta and org-level results.")
meta_paths <- expected_files |>
  dplyr::filter(category %in% c("meta", "organization_type")) |>
  dplyr::filter(exists)

for (filetype in unique(meta_paths[["filename"]])) {
  logger::log_info("Downloading {filetype} files.")
  these_files <- meta_paths |>
    dplyr::filter(filename == filetype) |>
    dplyr::pull(filepath)
  AzureStor::multidownload_blob(
    container,
    src = these_files,
    dest = these_files,
    overwrite = TRUE
  )
  logger::log_info("Combining {filetype} files.")
  contents <- NULL
  for (file in these_files) {
    logger::log_debug("Reading {file}.")
    this_meta <- expected_files |>
      dplyr::filter(filepath == file) |>
      dplyr::select(investor_name, portfolio_name)
    this_content <- readRDS(file) |>
      mutate(
        investor_name = this_meta[["investor_name"]],
        portfolio_name = this_meta[["portfolio_name"]]
      )
    contents <- dplyr::bind_rows(contents, this_content)
  }
  logger::log_info("Saving combined {filetype} file.")
  org_output_dir <- file.path(combined_output_dir, "organization_type")
  if (!dir.exists(org_output_dir)) {
    dir.create(org_output_dir, recursive = TRUE)
  }
  saveRDS(contents, file.path(org_output_dir, filetype))
}

logger::log_info("Combining user-level results.")
user_paths <- expected_files |>
  dplyr::filter(category == "user_id") |>
  dplyr::filter(exists)

for (filetype in unique(user_paths[["filename"]])) {
  logger::log_info("Downloading {filetype} files.")
  these_files <- user_paths |>
    dplyr::filter(filename == filetype) |>
    dplyr::pull(filepath)
  AzureStor::multidownload_blob(
    container,
    src = these_files,
    dest = these_files,
    overwrite = TRUE
  )
  logger::log_info("Combining {filetype} files.")
  contents <- NULL
  for (file in these_files) {
    logger::log_debug("Reading {file}.")
    this_meta <- expected_files |>
      dplyr::filter(filepath == file) |>
      dplyr::select(investor_name, portfolio_name)
    this_content <- readRDS(file) |>
      mutate(
        investor_name = this_meta[["investor_name"]],
        portfolio_name = this_meta[["portfolio_name"]]
      )
    contents <- dplyr::bind_rows(contents, this_content)
  }
  logger::log_info("Saving combined {filetype} file.")
  user_output_dir <- file.path(combined_output_dir, "user_id")
  if (!dir.exists(user_output_dir)) {
    dir.create(user_output_dir, recursive = TRUE)
  }
  saveRDS(contents, file.path(user_output_dir, filetype))
}

file.copy(
  from = file.path(org_output_dir, "Equity_results_portfolio.rds"),
  to = file.path(combined_output_dir, paste0(project_code, "_peers_equity_results_portfolio.rds"))
)
file.copy(
  from = file.path(org_output_dir, "Bonds_results_portfolio.rds"),
  to = file.path(combined_output_dir, paste0(project_code, "_peers_bonds_results_portfolio.rds"))
)
file.copy(
  from = file.path(user_output_dir, "Equity_results_portfolio.rds"),
  to = file.path(combined_output_dir, paste0(project_code, "_peers_equity_results_portfolio_ind.rds"))
)
file.copy(
  from = file.path(user_output_dir, "Bonds_results_portfolio.rds"),
  to = file.path(combined_output_dir, paste0(project_code, "_peers_bonds_results_portfolio_ind.rds"))
)

