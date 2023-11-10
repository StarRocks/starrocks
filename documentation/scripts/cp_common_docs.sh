#!/bin/bash

mkdir -p releasenotes
mkdir -p common

rm -rf versioned_docs/version-*/release_notes versioned_docs/version-*/developers
rm -rf i18n/zh/docusaurus-plugin-content-docs/version-*/release_notes i18n/zh/docusaurus-plugin-content-docs/version-*/developers
mkdir -p versioned_docs/version-3.1/developers/
mkdir -p i18n/zh/docusaurus-plugin-content-docs/version-3.1/developers/
mkdir -p versioned_docs/version-3.0/developers/
mkdir -p i18n/zh/docusaurus-plugin-content-docs/version-3.0/developers/
mkdir -p versioned_docs/version-2.5/developers/
mkdir -p i18n/zh/docusaurus-plugin-content-docs/version-2.5/developers/
mkdir -p versioned_docs/version-2.3/developers/
mkdir -p i18n/zh/docusaurus-plugin-content-docs/version-2.3/developers/
mkdir -p versioned_docs/version-2.2/developers/
mkdir -p i18n/zh/docusaurus-plugin-content-docs/version-2.2/developers/
mkdir -p versioned_docs/version-2.1/developers/
mkdir -p i18n/zh/docusaurus-plugin-content-docs/version-2.1/developers/
mkdir -p versioned_docs/version-2.0/developers/
mkdir -p i18n/zh/docusaurus-plugin-content-docs/version-2.0/developers/
mkdir -p versioned_docs/version-1.19/developers/
mkdir -p i18n/zh/docusaurus-plugin-content-docs/version-1.19/developers/

cp versioned_docs/version-3.1/assets/debug_info.png versioned_docs/version-3.0/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png versioned_docs/version-3.0/assets/
cp versioned_docs/version-3.1/assets/ide*.png versioned_docs/version-3.0/assets/
cp versioned_docs/version-3.1/assets/trace*.png versioned_docs/version-3.0/assets/
cp versioned_docs/version-3.1/assets/debug_info.png versioned_docs/version-2.5/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png versioned_docs/version-2.5/assets/
cp versioned_docs/version-3.1/assets/ide*.png versioned_docs/version-2.5/assets/
cp versioned_docs/version-3.1/assets/trace*.png versioned_docs/version-2.5/assets/
cp versioned_docs/version-3.1/assets/debug_info.png versioned_docs/version-2.3/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png versioned_docs/version-2.3/assets/
cp versioned_docs/version-3.1/assets/ide*.png versioned_docs/version-2.3/assets/
cp versioned_docs/version-3.1/assets/trace*.png versioned_docs/version-2.3/assets/
cp versioned_docs/version-3.1/assets/debug_info.png versioned_docs/version-2.2/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png versioned_docs/version-2.2/assets/
cp versioned_docs/version-3.1/assets/ide*.png versioned_docs/version-2.2/assets/
cp versioned_docs/version-3.1/assets/trace*.png versioned_docs/version-2.2/assets/
cp versioned_docs/version-3.1/assets/debug_info.png versioned_docs/version-2.1/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png versioned_docs/version-2.1/assets/
cp versioned_docs/version-3.1/assets/ide*.png versioned_docs/version-2.1/assets/
cp versioned_docs/version-3.1/assets/trace*.png versioned_docs/version-2.1/assets/
cp versioned_docs/version-3.1/assets/debug_info.png versioned_docs/version-2.0/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png versioned_docs/version-2.0/assets/
cp versioned_docs/version-3.1/assets/ide*.png versioned_docs/version-2.0/assets/
cp versioned_docs/version-3.1/assets/trace*.png versioned_docs/version-2.0/assets/
cp versioned_docs/version-3.1/assets/debug_info.png versioned_docs/version-1.19/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png versioned_docs/version-1.19/assets/
cp versioned_docs/version-3.1/assets/ide*.png versioned_docs/version-1.19/assets/
cp versioned_docs/version-3.1/assets/trace*.png versioned_docs/version-1.19/assets/

cp versioned_docs/version-3.1/assets/debug_info.png i18n/zh/docusaurus-plugin-content-docs/version-3.1/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png i18n/zh/docusaurus-plugin-content-docs/version-3.1/assets/
cp versioned_docs/version-3.1/assets/ide*.png i18n/zh/docusaurus-plugin-content-docs/version-3.1/assets/
cp versioned_docs/version-3.1/assets/trace*.png i18n/zh/docusaurus-plugin-content-docs/version-3.1/assets/
cp versioned_docs/version-3.1/assets/debug_info.png i18n/zh/docusaurus-plugin-content-docs/version-3.0/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png i18n/zh/docusaurus-plugin-content-docs/version-3.0/assets/
cp versioned_docs/version-3.1/assets/ide*.png i18n/zh/docusaurus-plugin-content-docs/version-3.0/assets/
cp versioned_docs/version-3.1/assets/trace*.png i18n/zh/docusaurus-plugin-content-docs/version-3.0/assets/
cp versioned_docs/version-3.1/assets/debug_info.png i18n/zh/docusaurus-plugin-content-docs/version-2.5/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png i18n/zh/docusaurus-plugin-content-docs/version-2.5/assets/
cp versioned_docs/version-3.1/assets/ide*.png i18n/zh/docusaurus-plugin-content-docs/version-2.5/assets/
cp versioned_docs/version-3.1/assets/trace*.png i18n/zh/docusaurus-plugin-content-docs/version-2.5/assets/
cp versioned_docs/version-3.1/assets/debug_info.png i18n/zh/docusaurus-plugin-content-docs/version-2.3/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png i18n/zh/docusaurus-plugin-content-docs/version-2.3/assets/
cp versioned_docs/version-3.1/assets/ide*.png i18n/zh/docusaurus-plugin-content-docs/version-2.3/assets/
cp versioned_docs/version-3.1/assets/trace*.png i18n/zh/docusaurus-plugin-content-docs/version-2.3/assets/
cp versioned_docs/version-3.1/assets/debug_info.png i18n/zh/docusaurus-plugin-content-docs/version-2.2/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png i18n/zh/docusaurus-plugin-content-docs/version-2.2/assets/
cp versioned_docs/version-3.1/assets/ide*.png i18n/zh/docusaurus-plugin-content-docs/version-2.2/assets/
cp versioned_docs/version-3.1/assets/trace*.png i18n/zh/docusaurus-plugin-content-docs/version-2.2/assets/
cp versioned_docs/version-3.1/assets/debug_info.png i18n/zh/docusaurus-plugin-content-docs/version-2.1/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png i18n/zh/docusaurus-plugin-content-docs/version-2.1/assets/
cp versioned_docs/version-3.1/assets/ide*.png i18n/zh/docusaurus-plugin-content-docs/version-2.1/assets/
cp versioned_docs/version-3.1/assets/trace*.png i18n/zh/docusaurus-plugin-content-docs/version-2.1/assets/
cp versioned_docs/version-3.1/assets/debug_info.png i18n/zh/docusaurus-plugin-content-docs/version-2.0/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png i18n/zh/docusaurus-plugin-content-docs/version-2.0/assets/
cp versioned_docs/version-3.1/assets/ide*.png i18n/zh/docusaurus-plugin-content-docs/version-2.0/assets/
cp versioned_docs/version-3.1/assets/trace*.png i18n/zh/docusaurus-plugin-content-docs/version-2.0/assets/
cp versioned_docs/version-3.1/assets/debug_info.png i18n/zh/docusaurus-plugin-content-docs/version-1.19/assets/
cp versioned_docs/version-3.1/assets/IDEA*.png i18n/zh/docusaurus-plugin-content-docs/version-1.19/assets/
cp versioned_docs/version-3.1/assets/ide*.png i18n/zh/docusaurus-plugin-content-docs/version-1.19/assets/
cp versioned_docs/version-3.1/assets/trace*.png i18n/zh/docusaurus-plugin-content-docs/version-1.19/assets/

mv common/releasenotes/en-us/release*.md releasenotes/
mv common/releasenotes/en-us/*connector.md releasenotes/

mv common/releasenotes/zh-cn/release*.md i18n/zh/docusaurus-plugin-content-docs-releasenotes/current/
mv common/releasenotes/zh-cn/*connector.md i18n/zh/docusaurus-plugin-content-docs-releasenotes/current/

cp -r common/releasenotes/en-us/build-starrocks versioned_docs/version-3.1/developers/
cp -r common/releasenotes/en-us/code-style-guides versioned_docs/version-3.1/developers/
cp common/releasenotes/en-us/debuginfo.md versioned_docs/version-3.1/developers/
cp -r common/releasenotes/en-us/development-environment versioned_docs/version-3.1/developers/
cp -r common/releasenotes/en-us/trace-tools versioned_docs/version-3.1/developers/

cp -r common/releasenotes/zh-cn/build-starrocks i18n/zh/docusaurus-plugin-content-docs/version-3.1/developers/
cp -r common/releasenotes/en-us/code-style-guides i18n/zh/docusaurus-plugin-content-docs/version-3.1/developers/
cp common/releasenotes/zh-cn/debuginfo.md i18n/zh/docusaurus-plugin-content-docs/version-3.1/developers/
cp -r common/releasenotes/en-us/development-environment i18n/zh/docusaurus-plugin-content-docs/version-3.1/developers/
cp -r common/releasenotes/en-us/trace-tools i18n/zh/docusaurus-plugin-content-docs/version-3.1/developers/

cp -r common/releasenotes/en-us/build-starrocks versioned_docs/version-3.0/developers/
cp -r common/releasenotes/en-us/code-style-guides versioned_docs/version-3.0/developers/
cp common/releasenotes/en-us/debuginfo.md versioned_docs/version-3.0/developers/
cp -r common/releasenotes/en-us/development-environment versioned_docs/version-3.0/developers/
cp -r common/releasenotes/en-us/trace-tools versioned_docs/version-3.0/developers/

cp -r common/releasenotes/zh-cn/build-starrocks i18n/zh/docusaurus-plugin-content-docs/version-3.0/developers/
cp -r common/releasenotes/en-us/code-style-guides i18n/zh/docusaurus-plugin-content-docs/version-3.0/developers/
cp common/releasenotes/zh-cn/debuginfo.md i18n/zh/docusaurus-plugin-content-docs/version-3.0/developers/
cp -r common/releasenotes/en-us/development-environment i18n/zh/docusaurus-plugin-content-docs/version-3.0/developers/
cp -r common/releasenotes/en-us/trace-tools i18n/zh/docusaurus-plugin-content-docs/version-3.0/developers/

cp -r common/releasenotes/en-us/build-starrocks versioned_docs/version-2.5/developers/
cp -r common/releasenotes/en-us/code-style-guides versioned_docs/version-2.5/developers/
cp common/releasenotes/en-us/debuginfo.md versioned_docs/version-2.5/developers/
cp -r common/releasenotes/en-us/development-environment versioned_docs/version-2.5/developers/
cp -r common/releasenotes/en-us/trace-tools versioned_docs/version-2.5/developers/

cp -r common/releasenotes/zh-cn/build-starrocks i18n/zh/docusaurus-plugin-content-docs/version-2.5/developers/
cp -r common/releasenotes/en-us/code-style-guides i18n/zh/docusaurus-plugin-content-docs/version-2.5/developers/
cp common/releasenotes/zh-cn/debuginfo.md i18n/zh/docusaurus-plugin-content-docs/version-2.5/developers/
cp -r common/releasenotes/en-us/development-environment i18n/zh/docusaurus-plugin-content-docs/version-2.5/developers/
cp -r common/releasenotes/en-us/trace-tools i18n/zh/docusaurus-plugin-content-docs/version-2.5/developers/

cp -r common/releasenotes/en-us/build-starrocks versioned_docs/version-2.3/developers/
cp -r common/releasenotes/en-us/code-style-guides versioned_docs/version-2.3/developers/
cp common/releasenotes/en-us/debuginfo.md versioned_docs/version-2.3/developers/
cp -r common/releasenotes/en-us/development-environment versioned_docs/version-2.3/developers/
cp -r common/releasenotes/en-us/trace-tools versioned_docs/version-2.3/developers/

cp -r common/releasenotes/zh-cn/build-starrocks i18n/zh/docusaurus-plugin-content-docs/version-2.3/developers/
cp -r common/releasenotes/en-us/code-style-guides i18n/zh/docusaurus-plugin-content-docs/version-2.3/developers/
cp common/releasenotes/zh-cn/debuginfo.md i18n/zh/docusaurus-plugin-content-docs/version-2.3/developers/
cp -r common/releasenotes/en-us/development-environment i18n/zh/docusaurus-plugin-content-docs/version-2.3/developers/
cp -r common/releasenotes/en-us/trace-tools i18n/zh/docusaurus-plugin-content-docs/version-2.3/developers/

cp -r common/releasenotes/en-us/build-starrocks versioned_docs/version-2.2/developers/
cp -r common/releasenotes/en-us/code-style-guides versioned_docs/version-2.2/developers/
cp common/releasenotes/en-us/debuginfo.md versioned_docs/version-2.2/developers/
cp -r common/releasenotes/en-us/development-environment versioned_docs/version-2.2/developers/
cp -r common/releasenotes/en-us/trace-tools versioned_docs/version-2.2/developers/

cp -r common/releasenotes/zh-cn/build-starrocks i18n/zh/docusaurus-plugin-content-docs/version-2.2/developers/
cp -r common/releasenotes/en-us/code-style-guides i18n/zh/docusaurus-plugin-content-docs/version-2.2/developers/
cp common/releasenotes/zh-cn/debuginfo.md i18n/zh/docusaurus-plugin-content-docs/version-2.2/developers/
cp -r common/releasenotes/en-us/development-environment i18n/zh/docusaurus-plugin-content-docs/version-2.2/developers/
cp -r common/releasenotes/en-us/trace-tools i18n/zh/docusaurus-plugin-content-docs/version-2.2/developers/

cp -r common/releasenotes/en-us/build-starrocks versioned_docs/version-2.1/developers/
cp -r common/releasenotes/en-us/code-style-guides versioned_docs/version-2.1/developers/
cp common/releasenotes/en-us/debuginfo.md versioned_docs/version-2.1/developers/
cp -r common/releasenotes/en-us/development-environment versioned_docs/version-2.1/developers/
cp -r common/releasenotes/en-us/trace-tools versioned_docs/version-2.1/developers/

cp -r common/releasenotes/zh-cn/build-starrocks i18n/zh/docusaurus-plugin-content-docs/version-2.1/developers/
cp -r common/releasenotes/en-us/code-style-guides i18n/zh/docusaurus-plugin-content-docs/version-2.1/developers/
cp common/releasenotes/zh-cn/debuginfo.md i18n/zh/docusaurus-plugin-content-docs/version-2.1/developers/
cp -r common/releasenotes/en-us/development-environment i18n/zh/docusaurus-plugin-content-docs/version-2.1/developers/
cp -r common/releasenotes/en-us/trace-tools i18n/zh/docusaurus-plugin-content-docs/version-2.1/developers/

cp -r common/releasenotes/en-us/build-starrocks versioned_docs/version-2.0/developers/
cp -r common/releasenotes/en-us/code-style-guides versioned_docs/version-2.0/developers/
cp common/releasenotes/en-us/debuginfo.md versioned_docs/version-2.0/developers/
cp -r common/releasenotes/en-us/development-environment versioned_docs/version-2.0/developers/
cp -r common/releasenotes/en-us/trace-tools versioned_docs/version-2.0/developers/

cp -r common/releasenotes/zh-cn/build-starrocks i18n/zh/docusaurus-plugin-content-docs/version-2.0/developers/
cp -r common/releasenotes/en-us/code-style-guides i18n/zh/docusaurus-plugin-content-docs/version-2.0/developers/
cp common/releasenotes/zh-cn/debuginfo.md i18n/zh/docusaurus-plugin-content-docs/version-2.0/developers/
cp -r common/releasenotes/en-us/development-environment i18n/zh/docusaurus-plugin-content-docs/version-2.0/developers/
cp -r common/releasenotes/en-us/trace-tools i18n/zh/docusaurus-plugin-content-docs/version-2.0/developers/

cp -r common/releasenotes/en-us/build-starrocks versioned_docs/version-1.19/developers/
cp -r common/releasenotes/en-us/code-style-guides versioned_docs/version-1.19/developers/
cp common/releasenotes/en-us/debuginfo.md versioned_docs/version-1.19/developers/
cp -r common/releasenotes/en-us/development-environment versioned_docs/version-1.19/developers/
cp -r common/releasenotes/en-us/trace-tools versioned_docs/version-1.19/developers/

cp -r common/releasenotes/zh-cn/build-starrocks i18n/zh/docusaurus-plugin-content-docs/version-1.19/developers/
cp -r common/releasenotes/en-us/code-style-guides i18n/zh/docusaurus-plugin-content-docs/version-1.19/developers/
cp common/releasenotes/zh-cn/debuginfo.md i18n/zh/docusaurus-plugin-content-docs/version-1.19/developers/
cp -r common/releasenotes/en-us/development-environment i18n/zh/docusaurus-plugin-content-docs/version-1.19/developers/
cp -r common/releasenotes/en-us/trace-tools i18n/zh/docusaurus-plugin-content-docs/version-1.19/developers/
