# Generate PDFs from the StarRocks Docusaurus documentation site

Node.js code to:
1. Generate the ordered list of URLs from the documentation. This is done using code from [`docusaurus-prince-pdf`](https://github.com/signcl/docusaurus-prince-pdf).
2. Convert each page to a PDF file with [`Gotenberg`](https://pptr.dev/).
3. Combine the individual PDF files using [Ghostscript](https://www.ghostscript.com/) and [`pdfcombine`](https://github.com/tdegeus/pdfcombine.git).

## Onetime setup

### Clone this repo

Clone this repo to your machine.

### The conversion environment

The conversion process uses Docker Compose. Launch the environment by running the following command from the `starrocks/docs/docusaurus/PDF/` directory.

```bash
docker compose up --detach --wait --wait-timeout 120 --build
```

> Tip
>
> All of the `docker compose` commands must be run from the `starrocks/docs/docusaurus/PDF/` directory.

Check the status:

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

### Open a shell in the PDF build environment

Launch a shell from the `starrocks/docs/docusaurus/PDF` directory:

```bash
docker compose exec -ti docusaurus bash
```

### Crawl

The Docker Compose environment has two services:
- `docusaurus`
- `gotenberg`

At a high level the process to create the PDF files is to:

- Generate a list of URLs
- for each URL in the list, signal the `gotenberg` service to connect to the `docusaurus` service and generate a PDF stream
- Write the PDF stream to disk

### Generate a list of pages (URLs)

```bash
http://docusaurus:3000/zh/docs/introduction/StarRocks_intro/
```


This command will crawl the docs and list the URLs in order:

```bash
npm install -g docusaurus-prince-pdf
npx docusaurus-prince-pdf --list-only -u http://docusaurus:3000/zh/docs/introduction/StarRocks_intro/ --file URLs.txt
```

<details>
  <summary>Expand to see URLs.txt sample</summary>

This is the file format, using the StarRocks developer docs as an example:
```bash
http://docusaurus:3000/zh/docs/developers/build-starrocks/Build_in_docker/
http://docusaurus:3000/zh/docs/developers/build-starrocks/build_starrocks_on_ubuntu/
http://docusaurus:3000/zh/docs/developers/build-starrocks/handbook/
http://docusaurus:3000/zh/docs/developers/code-style-guides/protobuf-guides/
http://docusaurus:3000/zh/docs/developers/code-style-guides/restful-api-standard/
http://docusaurus:3000/zh/docs/developers/code-style-guides/thrift-guides/
http://docusaurus:3000/zh/docs/developers/debuginfo/
http://docusaurus:3000/zh/docs/developers/development-environment/IDEA/
http://docusaurus:3000/zh/docs/developers/development-environment/ide-setup/
http://docusaurus:3000/zh/docs/developers/trace-tools/Trace/%
```

</details>


## docusaurus-puppeteer-pdf.js

This takes the URLs.txt generated above and:
1. creates PDF files for each URL in the file
2. creates the file `combine.yaml` which contains the titles of the pages and filenames. This is the input to the next step.

```bash
node docusaurus-puppeteer-pdf.js
```

> Note:
>
> Some characters in Markdown titles cause problems. The code has been written to remove square brackets (`[`, `]`) and colons (`:`) in titles as these were causing errors when running `pdfcombine` with the StarRocks docs. If you see errors when running `pdfcombine` you may have to edit `combine.yaml` and remove the offending characters.
>
> Open an issue in this repo and send your `combine.yaml` if you need help.

## Join the individual PDF files

```bash
source .venv/bin/activate
pdfcombine -y combine.yaml --title="StarRocks 2.5" -o StarRocks_2.5.pdf
```

> Note:
>
> You may see this message during the `pdfcombine` step:
>
> `GPL Ghostscript 10.03.1: Missing glyph CID=93, glyph=005d in the font IAAAAA+Menlo-Regular . The output PDF may fail with some viewers.`
>
> I have not had any complaints about the missing glyph from readers of the documents produced with this.

## Customizing the docs site for PDF

Some things do not make sense to have in the PDF, like the Feedback form at the bottom of the page. Removing the Feedback form from the PDF can be done with CSS. This snippet is added to the Docusaurus CSS file `src/css/custom.css`:

```css
/* When we generate PDF files we do not need to show the feedback widget. */
@media print {
    .feedback_Ak7m {
        display: none;
    }
}
```
