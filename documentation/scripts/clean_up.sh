#!/bin/bash

echo "cleanup before running yarn build"
find . -type f -name TOC.md | xargs rm
rm i18n/zh/docusaurus-plugin-content-docs/version-*/README.md
rm versioned_docs/version-*/README.md

rm -rf i18n/zh/docusaurus-plugin-content-docs/version-*/ecosystem_release/
rm -rf versioned_docs/version-*/ecosystem_release/

echo "replacing StarRocks intro page\n\n"
cp _IGNORE/_StarRocks_intro_Chinese.mdx i18n/zh/docusaurus-plugin-content-docs/version-3.1/introduction/StarRocks_intro.md
cp _IGNORE/_StarRocks_intro_Chinese.mdx i18n/zh/docusaurus-plugin-content-docs/version-3.0/introduction/StarRocks_intro.md
cp _IGNORE/_StarRocks_intro_Chinese.mdx i18n/zh/docusaurus-plugin-content-docs/version-2.5/introduction/StarRocks_intro.md
rm i18n/zh/docusaurus-plugin-content-docs/version-2.3/introduction/StarRocks_intro.md
cp _IGNORE/_StarRocks_intro_English.mdx versioned_docs/version-3.1/introduction/StarRocks_intro.md
cp _IGNORE/_StarRocks_intro_English.mdx versioned_docs/version-3.0/introduction/StarRocks_intro.md
cp _IGNORE/_StarRocks_intro_English.mdx versioned_docs/version-2.5/introduction/StarRocks_intro.md
rm versioned_docs/version-2.3/introduction/StarRocks_intro.md

echo "\nadd placeholders for pages in Chinese docs but not in English docs"
_IGNORE/add_missing_english_files.sh

echo "\nadd index pages"
_IGNORE/cp_index_pages.sh

echo "\nadd release notes and dev pages"
_IGNORE/cp_common_docs.sh

echo "\nadding frontmatter for sidebar language"
_IGNORE/add_chinese_sidebar.sh
_IGNORE/add_english_sidebar.sh

echo "verifying Markdown"
npx docusaurus-mdx-checker -c versioned_docs
npx docusaurus-mdx-checker -c i18n

echo "removing temp files"
rm -rf temp
