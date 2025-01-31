#! /usr/bin/bash
cd /app

rm -rf docusaurus/

mkdir -p docusaurus/docs

# Do not use these BYOC docs (use open-source instead):
#rm temp/byoc-temp/integrations/authenticate_to_aws_resources

# Copy in the open-source docs
cp -r temp/oss-temp/* docusaurus/docs/

# Remove open-source docs that will not be used and not overwritten
rm -rf docusaurus/docs/deployment
rm docusaurus/docs/introduction/what_is_starrocks.md

# Overwrite the open-source docs with BYOC docs
# (except for the ones removed in "Do not use these BYOC docs")
#cp -r temp/byoc-temp/* docusaurus/docs/

# Overwrite again with bare-metal docs
cp -r temp/bare-metal-temp/* docusaurus/docs/

# We need the plan-cluster doc
# Rename it to 15_ so it comes in right after install.
cp temp/oss-temp/deployment/plan_cluster.md docusaurus/docs/deployment/15_plan_cluster.md

rm -f docusaurus/docs/README.md

# Remove open-source docs that we do not publish in bare-metal
rm -rf docusaurus/docs/quick_start
rm -rf docusaurus/docs/cover_pages
rm -rf docusaurus/docs/project_help
rm -rf docusaurus/docs/developers
rm -rf docusaurus/docs/release_notes
rm -rf docusaurus/docs/ecosystem_release
rm docusaurus/docs/data_source/icebergtutorial.mdx
rm docusaurus/docs/introduction/StarRocks_intro.md
rm docusaurus/docs/administration/stargo.md
rm -rf docusaurus/docs/integrations/other_integrations
rm docusaurus/docs/integrations/streaming.mdx
rm docusaurus/docs/loading/Json_loading.md
rm docusaurus/docs/loading/loading.mdx
rm docusaurus/docs/loading/loading_introduction/loading_overview.mdx
rm docusaurus/docs/loading/objectstorage.mdx
rm docusaurus/docs/unloading/unloading.mdx

cp -r \
  temp/config/docusaurus.config.js \
  temp/config/package.json \
  temp/config/yarn.lock \
  temp/config/babel.config.js \
  temp/config/sidebars.js* \
  temp/config/src \
  temp/config/static \
docusaurus/

cd docusaurus/

find docs -name "*.md*" | xargs -d "\n" sed -i "s/displayed_sidebar:.*//"

find . -regex '.*\.\(mdx\|md\)$' \
  -exec grep -q '^release_status: DEPRECATED$' '{}' ';' \
  -delete

yarn install --frozen-lockfile
yarn clear && yarn build && yarn serve -p 3000 -h 0.0.0.0
cd /app

