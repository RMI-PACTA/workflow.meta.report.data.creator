# Meta Report Data Creator

[![Project Status: Unsupported – The project has reached a stable, usable state but the author(s) have ceased all work on it. A new maintainer may be desired.](https://www.repostatus.org/badges/latest/unsupported.svg)](https://www.repostatus.org/#unsupported)

**This project is archived for future reference, but no new work is expected in this repository. Future work is expected to be based off [`workflow.pacta`](https://github.com/RMI-PACTA/workflow.pacta).**

## Instructions

1. Phase 1: Prepare portfolio files
    1. Download the the initiavtive Download package from the CTM platform, and unzip it.
    2. Prepare a `config.yml` file, by filling out `template.yml`
    3. Prepare the docker image
    4. Prepare the SAS
    5. `export STORAGE_ACCOUNT_SAS`
    6. Run phase 1 script
2. Phase 2: Run PACTA against portfolios
    1. Deploy

    ```sh
    az deployment group create \
      --resource-group "RMI-SP-PACTA-DEV" \
      --template-file azure-deploy.json \
      --parameters azure-deploy.parameters.json
    ```
    
    2. Remove (delete) container groups when finished
3. Phase 3: Prepare peer files
    5. `export STORAGE_ACCOUNT_SAS`
    6. Run phase 3 script

### Preparing the Docker image:

1. `az acr login -n transitionmonitordockerregistry`
2. Update `Dockerfile`'s `FROM` line to use the appropriate tag of the `workflow.transition.monitor` docker image (should be a private image, including data).
3. `docker build . -t "transitionmonitordockerregistry.azurecr.io/workflow.meta.report.data.creator:$(date +'%Y%m%dT%H%M')"`
4. `docker push` the image you just built

Take note of the tag generated for use during the deploy step.


### Preparing Storage Account and Generating the SAS

From an existing Azure Storage Account (in this example, `pactaportfolios`), create a Queue and a Blob Container with the `project_name` in lowercase (e.g. `pa2024ch`, not `PA2024CH`).

Then under "Security + Networking" / "Shared Access signature", generate an SAS with the following permissions:

- Allowed Services: `Blob`, `Queue`
- Allowed Resource Types: `Container`, `Object`
- Allwed Permissions: `Read`, `Write`, `List`, `Add`, `Create`, `Update`, `Process`

Generate an SAS for the storage account. It gets passed to the Azure Deploy Script.
An expiration time of ~ 72 hours should be enough to handle most projects.

Copy the SAS token (starts with `sv=`) somewhere safe.
You will need it for all three phases of the process.

### Phase 1

from the directory containing the unzipped initiative package and `config.yml`

**WARNING:** Make sure that the `STORAGE_ACCOUNT_SAS` envvar is available to the Rscript environment. (`export STORAGE_ACCOUNT_SAS="sv=blahblahblah"`)

**WARNING:** You may need to add your IP to the fiewall allow rules (under "networking")

```bash
Rscript /path/to/script/phase-1_combine-portfolios.R config.yml
```

This takes a few minutes for a reasonable-sized project (a few thousand portfolios).

### Deploy (phase 2)

```sh
az deployment group create \
  --resource-group "RMI-SP-PACTA-DEV" \
  --template-file azure-deploy.json \
  --parameters azure-deploy.parameters.json
```

Answer the questions that `az` provides.
Notably, the SAS can be copy/pasted into the field (but will not show).

### Phase 3

```bash
Rscript phase-3_combine-results.R
```

combines the results into peer files (meta + org in one set, user_id in the other).
Again, it needs the SAS available as an envvar to the R session, so that it can download the PACTA results generated in phase 2.
