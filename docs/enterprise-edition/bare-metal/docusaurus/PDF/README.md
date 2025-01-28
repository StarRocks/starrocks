# PDF generation

The only difference between generating PDFs for the Enterprise version and open-source are:

- the directory names
- the `.env` file

Use the code in the [open-source PDF dir](../../../../docusaurus/PDF/README.md)

## Directory

The open-source PDF dir is:

```bash
celerdata-enterprise/docs/docusaurus/PDF/
```

## `.env` file

1. Copy the `.env.enterprise` file from this directory to the open source `celerdata-enterprise/docs/docusaurus/PDF/.env` location

2. Customize the .env file for the version number

```bash
COVER_IMAGE=../../enterprise-edition/bare-metal/docusaurus/PDF/CelerData.png
COVER_TITLE="CelerData Enterprise v3.3"
COPYRIGHT="Copyright (c) 2025 CelerData, Inc."
```


