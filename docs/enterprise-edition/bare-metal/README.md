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

## Prerequisites

The prerequisites on a Mac are pretty slim, just the few listed here. If you are running on a Linux machine, then there are more. See the detailed setup at the end of this document for RHEL.

### Docker

### Node

Node.js version 20 is required. At the time this system was built v20 is the current LTS version. Other versions may fail with:

```bash
import pkg from './package.json' assert { type: 'json' };
```

### `yarn`

```bash
npm install --global yarn
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

> Tip:
> If you see a message `npx: command not found` then you probably do not have `node` installed or activated. If you installed `nvm` then try `nvm use 20` and then run the `npx` command again.

Linux only: Add a newline ot the end of the file:

```bash
sed -i -e '$a\' file
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

## Deploying on RHEL in a data center (headless)

Because the PDFs for the commercial products are generated by the release team (QA) the process needs to run on an x86-64 system. The Docs team uses Macs, so things are a little different. These instructions are written for RHEL 9 (AWS does not have a RHEN 7 AMI any more).

### RHEL 9 setup in AWS EC2

1. Deploy a RHEL 9 machine with 8GB RAM and 30GB storage
1. Install packages needed to generate the PDF:

    There are several packages needed to be able to run Puppeteer from Google to generate the PDFs. These are the packages for RHEL 9. I suspect that they are the same for CentOs 7, but have no way of verifying this. It seems that Google only publishes lists for Debian-like systems. One package, `pdftk-java`, is in the Extra Packages for Enterprise Linux (EPEL), and that repo is enabled by installing the link from Fedora:
    ```bash
    sudo dnf install https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm
    sudo dnf install atk at-spi2-atk libXcomposite-devel libXdamage libXrandr mesa-libgbm \
         libxkbcommon alsa-lib pdftk-java
    ```
1. Install git `sudo dnf install git`
1. [Create a user](https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/configuring_basic_system_settings/managing-users-and-groups_configuring-basic-system-settings#proc_managing-accounts-and-groups-using-command-line-tools_assembly_getting-started-with-managing-user-accounts) to run the process (a user `jenkins` is used to match the QA setup)
1. [Install Docker](https://docs.docker.com/engine/install/rhel/)
1. Add `jenkins` to the `docker` group (see the [Linux post-installation](https://docs.docker.com/engine/install/linux-postinstall/) docs)
1. Create the dir and copy your public key to the `/home/jenkins/.ssh.authorized_keys` file, chmod the dir and file, test ssh connection.
1. As user `jenkins` test `docker run hello-world`
1. As user `jenkins` [set up nvm](https://github.com/nvm-sh/nvm?tab=readme-ov-file#installing-and-updating)
1. Prevent `nvm` from being used all of the time

   Edit the user `jenkins` `.bashrc` and add the `no-use` argument (see the install docs for nvm linked above) to avoid using `nvm` all of the time, or set the default to the global default node version on your machine.

    This is the `~/.bashrc` on my test machine:

    ```bash
    export NVM_DIR="$([ -z "${XDG_CONFIG_HOME-}" ] && printf %s "${HOME}/.nvm" || printf %s "${XDG_CONFIG_HOME}/nvm")"
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh" --no-use # This loads nvm, without auto-using the default version
    ```
1. Logout and back in as `jenkins`
1. Check the `node` version

    The default should be the old version, 14 in the case of our CI machine:

    ```bash
    node --version
    ```

    Install version 20
    ```bash
    nvm install 20
    nvm use 20
    ```

    Check the version:
    ```bash
    node --version
    ```

    Verify that this change is only for the current shell instance by exiting and logging back in and:
    ```bash
    node --version
    ```
1. Clone the repos and go up to the top of this doc to build the PDF
