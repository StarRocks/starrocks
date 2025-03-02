# Generate PDFs of the CelerData Enterprise Bare Metal docs

## Clone this repo

Clone this repo to your machine.

## Working directory

Because the enterprise repo is based on a sync from the open-source repo the documentation directories are a little bit complicated.

```bash
celerdata-enterprise
├── docs
│   ├── PDFoutput
│   ├── docusaurus
│   ├── en
│   ├── enterprise-edition
│   │   ├── bare-metal       <-- Work in here
│   │   │   ├── docs
│   │   │   └── docusaurus
│   │   │       ├── PDF
│   │   │       ├── scripts
│   │   │       ├── src
│   │   │       └── static
│   │   └── kubernetes
│   ├── translation
│   └── zh
```

## Environment settings

In the future the build process may use files from the BYOC repo (so that we maintain the docs for Enterprise features in only one place). So, there is an environment file (`.env`) in the `bare-metal/docusaurus/` directory. Here is the sample:

```bash
OSS_DIR=/Users/droscign/GitHub/starrocks
BYOC_DIR=/Users/droscign/GitHub/celerdata-cloud-docs
BARE_METAL_DIR=/Users/droscign/GitHub/celerdata-enterprise/docs/enterprise-edition/bare-metal/docs
```

Copy the `.env.sample` to `.env` and replace the paths for the three repos to match where you have the `starrocks`, `celerdata-cloud-docs`, and `celerdata-enterprise` repos.

> Important:
>
> In the three repos being used, check out the branch for the docs you are building. For example, in each of the dirs:
>
> ```bash
> git switch branch-3.4`
> ```

## Launch the conversion environment

> Tip
>
> The two `scripts` commands must be run from the
>
> `celerdata-enterprise/docs/enterprise-edition/bare-metal/docusaurus/` directory.

### Build the Docker image

```bash
./scripts/docker-image.sh
```

### Build docs with Docusaurus

```bash
./scripts/docker-build-bare-metal.sh
```

## Build the PDF

### Get the URL of the "home" page

In the output of the Docker container you should see:

```bash
[SUCCESS] Serving "build" directory at: http://0.0.0.0:3000/
```

Open the URL with a browser and click the **Documentation** link in the top navigation. This will be the starting page, and you will need the starting page URL to generate the PDF.

### Generate a list of pages (URLs)

This command will crawl the docs and list the URLs in order:

> Tip
>
> The rest of the commands should be run from this directory:
>
> ```bash
> celerdata-enterprise/docs/enterprise-edition/bare-metal/docusaurus/PDF
> ```
>
> Substitute the URL you just copied for the URL below:

```bash
npx docusaurus-prince-pdf --list-only \
  --file URLs.txt \
  -u http://0.0.0.0:3000/docs/deployment/get_started/
```

<details>
  <summary>Expand to see URLs.txt sample</summary>

This is the file format:
```bash
http://0.0.0.0:3000/docs/overview_of_column_row_security/
http://0.0.0.0:3000/docs/masking_policy/
http://0.0.0.0:3000/docs/row_access_policy/
http://0.0.0.0:3000/docs/manage_priv/
http://0.0.0.0:3000/docs/map_ldap_group/
http://0.0.0.0:3000/docs/multi_warehouse/
http://0.0.0.0:3000/docs/failover_group/
http://0.0.0.0:3000/docs/runtime_disk_management/
http://0.0.0.0:3000/docs/enhanced_catalog/
http://0.0.0.0:3000/docs/auto_materialized_view/
http://0.0.0.0:3000/docs/transparent_data_encryption/
```

</details>

### Generate PDF files for each Docusaurus page

#### Environment

There is another `.env` file to edit for the PDF. Here is a sample using the file `CelerData.png` in the `docusaurus/PDF` directory for version 3.4:

```bash
COVER_IMAGE=./CelerData.png
COVER_TITLE="CelerData Enterprise Manager 3.4"
COPYRIGHT="Copyright (c) 2025 CelerData, Inc."
```

This reads the `.env` file and `URLs.txt` generated above and:
1. Creates a cover page
2. creates PDF files for each URL in the file

```bash
yarn install
node docusaurus-puppeteer-pdf.js
```

### Combine the individual PDFs

The previous step generated a PDF file for each Docusaurus page, combine the individual pages with `pdftk-java`:

> Tip
>
> Set the output filename to the proper version

```bash
pdftk 0*pdf output CelerDataManagerv3_4.pdf
```

### Cleanup

There are now hundreds of temporary PDF files in the directory, remove them with:

```bash
./clean
```

## Customizing the docs site for PDF

> Note:
>
> You should not need to customize this, open an issue for the docs team if you need to filter out a specific object from the PDF pages.

Some things do not make sense to have in the PDF, like the Feedback form at the bottom of the page. Removing the Feedback form from the PDF can be done with CSS. This snippet is added to the Docusaurus CSS file `docusaurus/src/css/custom.css`:

```css
/* When we generate PDF files:

 - avoid breaks in the middle of:
   - code blocks
   - admonitions (notes, tips, etc.)

 - we do not need to show the:
   - feedback widget.
   - edit this page
   - breadcrumbs

 */
@media print {
  .theme-code-block , .theme-admonition {
     break-inside: avoid;
  }
}

@media print {
    .theme-edit-this-page , .feedback_Ak7m , .theme-doc-breadcrumbs   {
        display: none;
    }
}
```
