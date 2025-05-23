# Docs

## The `docusaurus` dir

Docusaurus is used to generate HTML from the Markdown files and that is all. From there we generate PDF files of the pages with the Node.js code in the `PDF` subdirectory. See the README there for the details.

## Build and serve

Run these two commands from the `bare-metal/docusaurus` dir:

```bash
./scripts/docker-image.sh
./scripts/docker-build-bare-metal.sh
```

## Generate PDF

See the [main README](../README.md)
