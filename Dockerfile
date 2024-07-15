FROM transitionmonitordockerregistry.azurecr.io/rmi_pacta_2023q4_pa2024ch:20240711T081006Z

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
