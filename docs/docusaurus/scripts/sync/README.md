# sync

This is a copy of [sync.mjs](https://github.com/FranciscoMoretti/site/blob/ff6e161b174030b5ebfcc549d3c3d016e8f316e4/scripts/sync.mjs) from @FranciscoMoretti as decribed in [his blog](https://www.franciscomoretti.com/blog/syncing-files-to-public-folder-in-react-or-nextjs-using-chokidar-and-fs-extra).

This syncs files on update from the docs dirs (`docs/en/` and `docs/zh/`)
into the dirs that Docusaurus expects to read from (`docusaurus/docs/` and
`docusaurus/i18n/docusaurus-plugin-content-docs/current/`) so that we can edit the files where they belong in local clones of the GitHub repo and
see the results as files are saved in local runs of Docusaurus.

```shell
npm install
```

```shell
mkdir -p docs/en docs/zh
```

```shell
node sync.mjs --watch
```

