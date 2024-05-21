#! /bin/sh
rm -rf docs
mkdir docs
cp -r docs-temp/* docs
rm -rf docs/quick_start
rm -rf docs/benchmarking
rm -rf docs/developers
rm -rf docs/project_help
rm -rf docs/deployment
mkdir docs/deployment
echo "\# helm deployment here" > docs/deployment/helm.md
rm docs/data_source/iceberg*
mkdir docs/quick_start
cp ee-docs/quickstart/* docs/quick_start/
cp ee-docs/administration/management/* docs/administration/management/
cp ee-docs/components/Features/index.js src/components/Features/
cp ee-docs/branding/* static/img/
cp ee-docs/ee-docusaurus.config.js docusaurus.config.js
sed -i "s/: 'throw',/: 'warn',/" docusaurus.config.js
cp ee-docs/_assets/commonMarkdown/* docs/assets/commonMarkdown
mv docs/assets docs/_assets

find docs -name "*.md*" | xargs -d "\n" sed -i "s/\/assets\//\/_assets\//"

rm docs/cover_pages/deployment_preparation* docs/cover_pages/developers* docs/cover_pages/manage_deployment* docs/cover_pages/shared_nothing_deployment* 

yarn build --locale en && yarn serve -p 3000 -h 0.0.0.0
