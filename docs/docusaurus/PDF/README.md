# Generate a PDF version of the docs

This was developed to run on a Mac system with an M2 chip. Please open an issue if you try this on another architecture and have problems.

Node.js code to:
1. Generate the ordered list of URLs from documentation built with Docusaurus. This is done using code from [`docusaurus-prince-pdf`](https://github.com/signcl/docusaurus-prince-pdf)
2. Open each page with [`puppeteer`](https://pptr.dev/) and save the content (without nav or the footer) as a PDF file
3. Combine the individual PDF files using [pdftk-java](https://gitlab.com/pdftk-java/pdftk/-/blob/master/README.md?ref_type=heads)

## Onetime setup

### Clone this repo

Clone this repo to your machine.

### Node.js

This is tested with Node.js version 21.

Use Node.js version 21. You can install Node.js using the instructions at [nodejs.org](https://nodejs.org/en/download).

### Puppeteer

Add `puppeteer` and other dependencies by running this command in the repo directory `starrocks/docs/docusaurus/PDF/`.

```bash
yarn install
```

### pdftk-java

`pdftk-java` should be installed using Homebrew on a macOS system

```bash
brew install pdftk-java
```

## Use

### Configuration

There is a sample `.env` file, `.env.sample`, that you can copy to `.env`. This file specifies an image, title to place on the cover, and a Copyright notice. Here is the sample:

```bash
COVER_IMAGE=./StarRocks.png
COVER_TITLE="StarRocks 3.3"
COPYRIGHT="Copyright (c) 2024 The Linux Foundation"
```

- Copy `.env.sample` to `.env`
- Edit the file as needed

> Note:
>
> For the `COVER_IMAGE` Use a PNG or JPEG.

### Build your Docusaurus site and serve it

It seems to be necessary to run `yarn serve` rather than ~`yarn start`~ to have `docusaurus-prince-pdf` crawl the pages. I expect that there is a CSS class difference between development and production modes of Docusaurus.

If you are using the Docker scripts from [StarRocks](https://github.com/StarRocks/starrocks/tree/main/docs/docusaurus/scripts) then open another shell and:

```bash
cd starrocks/docs/docusaurus
./scripts/docker-image.sh && ./scripts/docker-build.sh
```

### Get the URL of the "home" page

Find the URL of the first page to crawl. It needs to be the landing, or home page of the site as the next step will generate a set of PDF files, one for each page of your site by extracting the landing page and looking for the "Next" button at the bottom right corner of each Docusaurus page. If you start from any page other than the first one, then you will only get a portion of the pages. For Chinese language StarRocks documentation served using the `./scripts/docker-build.sh` script this will be:

```bash
http://localhost:3000/zh/docs/introduction/StarRocks_intro/
```

### Generate a list of pages (URLs)

This command will crawl the docs and list the URLs in order:

> Tip
>
> The rest of the commands should be run from this directory:
>
> ```bash
> starrocks/docs/docusaurus/PDF/
> ```
>
> Substitute the URL you just copied for the URL below:

```bash
npx docusaurus-prince-pdf --list-only \
  --file URLs.txt \
  -u http://localhost:3000/zh/docs/introduction/StarRocks_intro/
```

<details>
  <summary>Expand to see URLs.txt sample</summary>

This is the file format, using the StarRocks developer docs as an example:
```bash
http://localhost:3000/zh/docs/developers/build-starrocks/Build_in_docker/
http://localhost:3000/zh/docs/developers/build-starrocks/build_starrocks_on_ubuntu/
http://localhost:3000/zh/docs/developers/build-starrocks/handbook/
http://localhost:3000/zh/docs/developers/code-style-guides/protobuf-guides/
http://localhost:3000/zh/docs/developers/code-style-guides/restful-api-standard/
http://localhost:3000/zh/docs/developers/code-style-guides/thrift-guides/
http://localhost:3000/zh/docs/developers/debuginfo/
http://localhost:3000/zh/docs/developers/development-environment/IDEA/
http://localhost:3000/zh/docs/developers/development-environment/ide-setup/
http://localhost:3000/zh/docs/developers/trace-tools/Trace/%
```

</details>


### Generate PDF files for each Docusaurus page

This reads the `URLs.txt` generated above and:
1. Creates a cover page
2. creates PDF files for each URL in the file

```bash
node docusaurus-puppeteer-pdf.js
```

### Combine the individual PDFs

The previous step generated a PDF file for each Docusaurus page, combine the individual pages with `pdftk-java`:

```bash
pdftk 0*pdf output docs.pdf
```

### Cleanup

There are now 900+ temporary PDF files in the directory, remove them with:

```bash
./clean
```

## Customizing the docs site for PDF

Some things do not make sense to have in the PDF, like the Feedback form at the bottom of the page. Removing the Feedback form from the PDF can be done with CSS. This snippet is added to the Docusaurus CSS file `docs/docusaurus/src/css/custom.css`:

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
