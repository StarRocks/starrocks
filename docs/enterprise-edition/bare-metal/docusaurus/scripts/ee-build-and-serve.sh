#! /bin/sh

# docusaurus config goes in workingdir and combined
# open-source and Enterprise docs go in workingdir/docs
mkdir -p workingdir/docs
cp -r docs/en/* workingdir/docs
# some open-source docs get removed
rm -rf workingdir/docs/quick_start
rm -rf workingdir/docs/benchmarking
rm -rf workingdir/docs/developers
rm -rf workingdir/docs/project_help
rm -rf workingdir/docs/deployment
mkdir workingdir/docs/deployment
echo "\# helm deployment here" > workingdir/docs/deployment/helm.md
rm workingdir/docs/data_source/iceberg*
mkdir workingdir/docs/quick_start
# Copy in the Docusaurus configs from open-source
cp -r ./docs/docusaurus/* workingdir/
# Add in the Enterprise specific docs
rsync -avhrW --progress ./docs/enterprise-edition/en/ workingdir/docs/
# Add Enterprise specific config for Docusaurus
rsync -avhrW --progress ./docs/enterprise-edition/components/ workingdir/src/components/
rsync -avhrW --progress ./docs/enterprise-edition/img/ workingdir/static/img/
cp ./docs/enterprise-edition/docusaurus.config.js workingdir/docusaurus.config.js
cp ./docs/enterprise-edition/sidebars.json workingdir/sidebars.json
sed -i "s/: 'throw',/: 'warn',/" workingdir/docusaurus.config.js
# hide the assets dir from nav and search
mv workingdir/docs/assets workingdir/docs/_assets
find workingdir/docs -name "*.md*" | xargs -d "\n" sed -i "s/\/assets\//\/_assets\//"
# Remove unused files
rm workingdir/docs/cover_pages/deployment_preparation* workingdir/docs/cover_pages/developers* workingdir/docs/cover_pages/manage_deployment* workingdir/docs/cover_pages/shared_nothing_deployment*
cd workingdir
yarn build && yarn serve -p 3000 -h 0.0.0.0
