FROM transitionmonitordockerregistry.azurecr.io/rmi_pacta_mfm_fall2024:1.1.0

# install R package dependencies
RUN Rscript -e "\
  pak::pak(c( \
    'AzureQstor', \
    'AzureStor', \
    'callr', \
    'logger' \
  )); \
  "

COPY run_pacta_queue.R /run_pacta_queue.R
CMD ["Rscript", "/run_pacta_queue.R"]
