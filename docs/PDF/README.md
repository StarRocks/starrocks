## Launch the services

The Docker compose file launches two services:
- `docusaurus`, which builds the docs and serves them on port 3000
- `docs-to-pdf`, which provides an environment that can run the `docs-to-pdf` NPM package. This
package is not easy to run on an M2 mac, as Puppeteer is not the easiest thing to run in general, and there is no Apple Silicon support in the version used by docs-to-pdf.

```bash
docker compose up --detach --wait --wait-timeout 120 --build
```

## Docusaurus
The Docusaurus build should start on its own. When you see the `[SUCCESS]` message it is ready. You can confirm this by opening a browser to [http://localhost:3000](http://localhost:3000)

```bash
docusaurus-1   | [SUCCESS] Serving "build" directory at: http://0.0.0.0:3000/
```

## PDF generation

Open a shell in the `docs-to-pdf` service:

```bash
docker compose exec -it docs-to-pdf bash
```

Once there confirm that Docusaurus is serving pages:

```bash
curl http://docusaurus:3000/docs/introduction/StarRocks_intro/
```

If you see HTML output, then Docusaurus is serving and you can start generating the PDF

```bash
npx docs-to-pdf \
  --initialDocURLs="http://docusaurus:3000/docs/introduction/StarRocks_intro/" \
  --contentSelector="article" \
  --paginationSelector="a.pagination-nav__link.pagination-nav__link--next" \
  --excludeSelectors=".margin-vert--xl a,[class^='tocCollapsible'],.breadcrumbs,.theme-edit-this-page" \
  --coverImage="https://docs.starrocks.io/img/logo.svg" \
  --coverTitle="StarRocks Documentation" \
  --pdfMargin="10,20,30,40" \
  --paperFormat="A4"
```

Answer `y` when prompted (I should install this in the image)

## Wait

It takes about 10 or 20 minutes for any output other than a spinning cursor to show up. Just let it run. Eventually you will see this output for each of our 900+ pages:

```bash
[07.11.2024 15:01.55.780] [LOG]   Retrieving html from http://docusaurus:3000/docs/loading/Stream_Load_transaction_interface/
[07.11.2024 15:01.57.258] [DEBUG] Found 0 elements
[07.11.2024 15:01.57.267] [LOG]   Success
```

