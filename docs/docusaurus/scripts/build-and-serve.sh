#! /bin/sh
mkdir ./docs &&
mkdir -p ./i18n/zh/docusaurus-plugin-content-docs/current &&
cp -r ./english/* ./docs/ &&
rm -rf ./docs/release_notes &&
cp -r ./chinese/* ./i18n/zh/docusaurus-plugin-content-docs/current &&
rm -rf ./i18n/zh/docusaurus-plugin-content-docs/current/release_notes &&
yarn build && yarn serve -p 3000 -h 0.0.0.0
