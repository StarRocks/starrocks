
## My working dir

```shell
cd GitHub/Docusaurus/starrocks
```

## Create the dirs and update the versions file
```shell
yarn docusaurus docs:version 3.0
yarn docusaurus docs:version 2.5
yarn docusaurus docs:version 2.4
```

## Create temp dirs to clone the repos in
```shell
mkdir temp-en
mkdir temp-zh
```

## Work on the English docs

```shell
cd temp-en
gh repo clone StarRocks/starrocks
```

### Process English version 3.1

```shell
cd starrocks
git checkout branch-3.1
rm -rf ../../versioned_docs/version-3.1
mkdir ../../versioned_docs/version-3.1
cp -r docs/* ../../versioned_docs/version-3.1/
```

### Process English version 3.0

```shell
git checkout branch-3.0
rm -rf ../../versioned_docs/version-3.0
mkdir ../../versioned_docs/version-3.0
cp -r docs/* ../../versioned_docs/version-3.0/
```

### Process English version 2.5

```shell
git checkout branch-2.5
rm -rf ../../versioned_docs/version-2.5
mkdir ../../versioned_docs/version-2.5
cp -r docs/* ../../versioned_docs/version-2.5
```

### Process English version 2.4

```shell
git checkout branch-2.4
rm -rf ../../versioned_docs/version-2.4
mkdir ../../versioned_docs/version-2.4
cp -r docs/* ../../versioned_docs/version-2.4/
```

## Work on the Chinese docs

```shell
cd GitHub/Docusaurus/starrocks/temp-zh
gh repo clone StarRocks/docs.zh-cn
cd docs.zh-cn
```

### Process Chinese version 3.1

```shell
git checkout 3.1
rm -rf ../../i18n/zh/docusaurus-plugin-content-docs/version-3.1/
mkdir ../../i18n/zh/docusaurus-plugin-content-docs/version-3.1
cp -r * ../../i18n/zh/docusaurus-plugin-content-docs/version-3.1/
```

### Process Chinese version 3.0

```shell
git checkout 3.0
rm -rf ../../i18n/zh/docusaurus-plugin-content-docs/version-3.0/
mkdir ../../i18n/zh/docusaurus-plugin-content-docs/version-3.0
cp -r * ../../i18n/zh/docusaurus-plugin-content-docs/version-3.0/
```

### Process Chinese version 2.5

```shell
git checkout 2.5
rm -rf ../../i18n/zh/docusaurus-plugin-content-docs/version-2.5/
mkdir ../../i18n/zh/docusaurus-plugin-content-docs/version-2.5
cp -r * ../../i18n/zh/docusaurus-plugin-content-docs/version-2.5
```

### Process Chinese version 2.4

```shell
git checkout 2.4
rm -rf ../../i18n/zh/docusaurus-plugin-content-docs/version-2.4
mkdir ../../i18n/zh/docusaurus-plugin-content-docs/version-2.4
cp -r * ../../i18n/zh/docusaurus-plugin-content-docs/version-2.4
```

## Back up to the working dir

```shell
cd ../../
```

## Check the files and correct our typical bad `<br`'s

```shell
npx docusaurus-mdx-checker
find versioned_docs -type f -name "*md"  -print0 | xargs -0 sed -i '' "s/<br>/<br \\/>/g"
find i18n/zh/docusaurus-plugin-content-docs -type f -name "*md"  -print0 | xargs -0 sed -i '' "s/<br>/<br \\/>/g"
```

## Check again and build

I skipped removing of about 50 markdown files with various problems (embedded HTML or bare `${ABC}` vars that
mdx will not allow.

```shell
npx docusaurus-mdx-checker
yarn build
```
