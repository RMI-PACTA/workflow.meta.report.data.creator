logger::log_info("Starting run_pacta_queue.R")

logger::log_info("preparing storage endpoint")
storage_endpoint <- AzureStor::storage_endpoint(
  "https://pactadatadev.blob.core.windows.net",
  sas = Sys.getenv("STORAGE_ACCOUNT_SAS")
)

project_code <- "Sys.getenv('PROJECT_CODE')"
container <- AzureStor::blob_container(storage_endpoint, project_code)

local_config_path <- "config.yml"

logger::log_info("Accessing config file")
AzureStor::download_blob(
  container,
  local_config_path,
  overwrite = TRUE
)

cfg <- config::get(file = local_config_path)

logger::log_info("Accessing queue")
queue_endpoint <- AzureStor::storage_endpoint(
  "https://pactadatadev.queue.core.windows.net",
  sas = Sys.getenv("STORAGE_ACCOUNT_SAS")
)

queue <- AzureQstor::storage_queue(
  queue_endpoint,
  project_code
)

logger::log_info("Getting message from queue")
msg <- queue$get_message()

working_dir_path <- "/bound/working_dir"

while (!is.null(msg$text)) {

  logger::log_info("Extending timeout for message")
  msg$update(cfg$timeout) # prevent from resurfacing before portfolio has time to run

  message_body <- jsonlite::fromJSON(msg$text)
  
  logger::log_info("Deleting files in working directory")
  unlink(list.files(working_dir_path, full.names = TRUE), recursive = TRUE)

  logger::log_info("Downloading blob")
  AzureStor::multidownload_blob(
    container = container,
    src = file.path(message_body$path, "*"),
    dest = working_dir_path,
    recursive = TRUE
  )

  if (cfg$run_results) {

    logger::log_info("running web_tool_script_1.R")
    callr::rscript(
      script = "/bound/web_tool_script_1.R",
      wd = "/bound",
      cmdargs = message_body$name,
      echo = TRUE,
      stderr = file.path(working_dir_path, "web_tool_script_1_stderr.txt")
    )


    logger::log_info("running web_tool_script_2.R")
    callr::rscript(
      script = "/bound/web_tool_script_2.R",
      wd = "/bound",
      cmdargs = message_body$name,
      stderr = "2>&1"
    )
  }

  if (cfg$run_reports) {
    logger::log_info("running web_tool_script_3.R")
    callr::rscript(
      script = "/bound/web_tool_script_3.R",
      wd = "/bound",
      cmdargs = message_body$name
    )
  }


  logger::log_info("marking message as finished")
  msg$delete()

  #actually get the next item
  logger::log_info("Getting message from queue")
  msg <- queue$get_message()
}

logger::log_info("Finished run_pacta_queue.R")
