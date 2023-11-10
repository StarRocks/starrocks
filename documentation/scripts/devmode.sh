#!/bin/bash

# remove previous run
rm -rf docs
rm -rf i18n/zh/docusaurus-plugin-content-docs/current

# create dirs for the docs from this PR
mkdir docs
mkdir i18n/zh/docusaurus-plugin-content-docs/current

# copy docs in place
# Note: Maybe the docs should just be in these dirs
# always? Is it confusing for contributors to look
# for the Chinese markdown files in i18n/zh.....
cp -r English/docs/* docs/
cp -r Chinese/docs/* i18n/zh/docusaurus-plugin-content-docs/current/

# set the env var to build current only
export DISABLE_VERSIONING=true

# install / update 
yarn install --frozen-lockfile
yarn start --locale ${DOCS_LOCALE}
