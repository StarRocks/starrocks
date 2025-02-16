# Docs

## The `docusaurus` dir

Docusaurus is used to generate HTML from the Markdown files and that is all. From there we generate PDF files of the pages with the Node.js code in the `PDF` subdirectory. See the README there for the details.

## Build and serve

Run these two commands from the `bare-metal/docusaurus` dir:

```bash
./scripts/docker-image.sh
./scripts/docker-build-bare-metal.sh
```












## To Do:

1. See where the docs in the bare-metal/docs dir belong. Some of them are probably the same as either BYOC or open-source docs and should be moved into the same place to make links work.

2. Fix these:

  ```bash
  cp: cannot stat 'temp/bare-metal-temp/administration/**': No such file or directory
  cp: cannot create regular file 'docusaurus/docs/loading/': Not a directory
  cp: target 'docusaurus/docs/table_design/': No such file or directory
  cp: cannot stat 'temp/bare-metal-temp/table_design/*': No such file or directory
  rm: cannot remove 'docusaurus/docs/table_design/table_types/table_capabilities.md': No such file or directory
  cp: target 'docusaurus/docs/using_starrocks/': No such file or directory
  cp: cannot stat 'temp/bare-metal-temp/using_starrocks/*': No such file or directory
  rm: cannot remove 'docusaurus/docs/README.md': No such file or directory
  ```

3. switch broken markdown links from `ignore` to `throw`

4. Get _category_.yml files in place for sql-ref, data_source, management, administration as they are showing in lower case.

5. Update the message shown at login to the correct script. I don't remember if this is an MOTD or something else.

