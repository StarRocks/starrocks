# Documenting the docs

The StarRocks documentation is:

- written in Markdown
- linted with `markdownlint` (very poorly, needs to be updated)
- checked with `docusaurus-mdx-checker`
- transformed to HTML with Docusaurus
- Stored in S3
- Served by CloudFront
- Indexed for search by Algolia Docsearch

## Set up your environment

Markdown is simple, but it is easy to produce difficult to read pages. Make sure that you review the look of your pages before submitting a PR. A Docker container and scripts are provided so that you can build the docs and see updates in the browser as you save changes to files.

Read the [README in the `docs/docusaurus`](../docusaurus/README.md) folder for details.

## Writing Markdown

### Recommended VS Code settings

#### Markdown link fixer

- **Markdown › Update Links On File Move: Enabled**

   Try to update links in Markdown files when a file is renamed/moved in the workspace. Use Markdown › Update Links On File Move: Include to configure which files trigger link updates.

- **Markdown › Update Links On File Move: Enable For Directories**

   Enable updating links when a directory is moved or renamed in the workspace.

- **Markdown › Update Links On File Move: Include**

   Glob patterns that specifies files that trigger automatic link updates. See Markdown › Update Links On File Move: Enabled for details about this feature.

   `**/*.{md,mdx,mkd,mdwn,mdown,markdown,markdn,mdtxt,mdtext,workbook}`

### Recommended VS Code plugins

- If you have this installed, turn it **OFF** as the file updating on rename/move is done in VS Code now without an extension. [Markdown Link Updater](https://marketplace.visualstudio.com/items?itemName=mathiassoeholm.markdown-link-updater)

## Indexing for search

Algolia Docsearch comes with community support. Use the [Algolia discord](https://discord.com/invite/W7kYfh7FKQ)

Algolia Docsearch crawls the docs daily. The search results presented are influenced greatly by the [index settings](./Algolia/index-settings.json). If you feel the need to modify the settings, do so slowly make one change at a time.
