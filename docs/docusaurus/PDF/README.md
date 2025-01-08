
# Generate PDFs from the StarRocks Docusaurus documentation site

Node.js code to:
1. Generate the ordered list of URLs from the documentation. This is done using code from `docusaurus-prince-pdf`.
2. Convert each page to a PDF file with Gotenberg.
3. Combine the individual PDF files using Ghostscript and `pdfcombine`.

## Clone this repo

Clone this repo to your machine.

## Choose the branch that you want a PDF for

When you launch the PDF conversion environment, it will use the active branch. So, if you want a PDF for version 3.3:

```bash
git switch branch-3.3
```

## Launch the conversion environment

The conversion process uses Docker Compose. Launch the environment by running the following command from the `starrocks/docs/docusaurus/PDF/` directory.

The `--wait-timeout 400` will give the services 400 seconds to get to a healthy state. This is to allow both Docusaurus and Gotenberg to become ready to handle requests. On my machine it takes about 200 seconds for Docusaurus to build the docs and start serving them.

```bash
cd starrocks/docs/docusaurus/PDF
docker compose up --detach --wait --wait-timeout 400 --build
```

> Tip
>
> All of the `docker compose` commands must be run from the `starrocks/docs/docusaurus/PDF/` directory.

## Check the status

> Tip
>
> If you do not have `jq` installed just run `docker compose ps`. The ouput using `jq` is easier to read, but you can get by with the more basic command.

```bash
docker compose ps --format json | jq '{Service: .Service, State: .State, Status: .Status}'
```

Expected output:

```bash
{
  "Service": "docusaurus",
  "State": "running",
  "Status": "Up 14 minutes"
}
{
  "Service": "gotenberg",
  "State": "running",
  "Status": "Up 2 hours (healthy)"
}
```

## Get the URL of the "home" page

### Check to see if Docusaurus is serving the pages

From the `PDF` directory check the logs of the `docusaurus` service:

```bash
docker compose logs -f docusaurus
```

When Docusaurus is ready you will see this line at the end of the log output:

```bash
docusaurus-1  | [SUCCESS] Serving "build" directory at: http://0.0.0.0:3000/
```

Stop watching the logs with CTRL-c

### Find the initial URL

First open the docs by launching a browser to the URL at the end of the log output, which should be [http://0.0.0.0:3000/](http://0.0.0.0:3000/).

Next, change to the Chinese documentation if you are generating a PDF document of the Chinese documentation.

Copy the URL of the starting page of the documentation that you would like to generate a PDF for.

Save the URL.

## Open a shell in the PDF build environment

Launch a shell from the `starrocks/docs/docusaurus/PDF` directory:

```bash
docker compose exec -ti docusaurus bash
```

and `cd` into the `PDF` directory:

```bash
cd /app/docusaurus/PDF
```

## Crawl the docs and generate the PDFs

Run the command:

> Tip
>
> The URL in the code sample is for the Chinese documentation, remove the `/zh/` if you want English.

```bash
node generatePdf.js http://0.0.0.0:3000/zh/docs/introduction/StarRocks_intro/
```

## Join the individual PDF files

> Note:
>
> Change the name of the PDF output file as needed, in the example this is `StarRocks_33`

```bash
cd ../../PDFoutput/
pdftk 00*pdf output StarRocks_33.pdf
```

## Finished file

The individual PDF files and the combined file will be on your local machine in `starrocks/docs/PDFoutput/`

## Customizing the docs site for PDF

Gotenberg generates the PDF files without the side navigation, header, and footer as these components are not displayed when the `media` is set to `print`. In our docs it does not make sense to have the breadcrumbs, edit URLs, or Feedback widget show. These are filtered out using CSS by adding `display: none` to the classes of these objects when `@media print`.

Removing the Feedback form from the PDF can be done with CSS. This snippet is added to the Docusaurus CSS file `src/css/custom.css`:

```css
/* When we generate PDF files we do not need to show the:
 - edit URL
 - Feedback widget
 - breadcrumbs
*/
@media print {
    .feedback_Ak7m {
        display: none;
    }

    .theme-doc-footer-edit-meta-row {
        display: none;
    };

    .breadcrumbs {
        display: none;
    };
}
```

## Links

- [`docusaurus-prince-pdf`](https://github.com/signcl/docusaurus-prince-pdf)
- [`Gotenberg`](https://pptr.dev/)
- [`pdftk`](https://gitlab.com/pdftk-java/pdftk)
- [Ghostscript](https://www.ghostscript.com/)
