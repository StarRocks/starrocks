# CelerData Enterprise docs

CelerData Enterprise is a product name. The product, CelerData Enterprise, consists of two components:

1. CelerData Server

   CelerData Server is a set of binaries that add the commercial features on top of open-source StarRocks. Do NOT call it Enterprise Edition or anything else, it is "CelerData Server"

2. CelerData Manager

   CelerData Manager is the UI component for deploying CelerData Server in VMs.

## Editing the docs

90+% of the CelerData Enterprise docs are copied from open-source. The remainder of the docs are in this repo at `docs/enterprise/`.

Edit the open-source docs in `starrocks/starrocks`.

Edit the Enterprise docs in `CelerData/celerdata-enterprise/docs/enterprise`

## Building the docs

cd into the root directory of this repo and run

```bash
./docs/enterprise/docusaurus/scripts/docker-image.sh
./docs/enterprise/docusaurus/scripts/docker-build-enterprise.sh
```

At the shell prompt run:

```bash
./enterprise-temp/docs/enterprise/docusaurus/scripts/build-enterprise.sh
```
