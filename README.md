# Meta Report Data Creator

[![Project Status: Unsupported – The project has reached a stable, usable state but the author(s) have ceased all work on it. A new maintainer may be desired.](https://www.repostatus.org/badges/latest/unsupported.svg)](https://www.repostatus.org/#unsupported)

**This project is archived for future reference, but no new work is expected in this repository. Future work is expected to be based off [`workflow.pacta`](https://github.com/RMI-PACTA/workflow.pacta).**

## Instructions

1. Download the thinitiavtive Download package from the CTM platform, and unzip it.
2. Prepare a `config.yml` file, by filling out `template.yml`
3. Prepare the docker image:
  Adjust the `FROM` line to point to whatever recent docker image you need it to be based on, then:
  ```sh
  az acr login -n transitionmonitordockerregistry
  docker build .
  ```
4. Prepare the SAS
5. `export STORAGE_ACCOUNT_SAS`
6. Run phase 1
7. Deploy

```sh
az deployment group create \
  --resource-group "RMI-SP-PACTA-DEV" \
  --template-file azure-deploy.json \
  --parameters azure-deploy.parameters.json
```

### Preparing the Docker image:

1. `az acr login -n transitionmonitordockerregistry`
2. Update `Dockerfile`'s `FROM` line to use the appropriate tag of the `workflow.transition.monitor` docker image (should be a private image, including data).
3. `docker build . -t "transitionmonitordockerregistry.azurecr.io/workflow.meta.report.data.creator:$(date +'%Y%m%dT%H%M')"`
4. `docker push` the image you just built

Take note of the tag generated for use during the deploy step.


### Generating the SAS

Generate an SAS for the storage account. It gets passed to the Azure Deploy Script.
An expiration time of ~ 72 hours should be enough to handle most projects.

### Deploy

In each of the R files (`phase-1`, `pahse-2`, and `phase-3`), update the lines defining:

* `data_path <- <path to unzipped directory from constructiva>`
* `output_dir <- <path to where meta results will live>`

Then run 
```bash
Rscript phase-1_combine-portfolios.R
```

If you encounter errors, you will need to modify the portfolio `csv` files to correct the errors.


Then run 
```bash
Rscript phase-2_run-pacta.R
```

which takes a long time.

```bash
Rscript phase-3_combine-results.R
```

combines the results into peer files (meta + org in one set, user_id in the other)
