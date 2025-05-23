export DOCUSAURUS_IGNORE_SSG_WARNINGS=true
export DISABLE_VERSIONING=true
export NODE_OPTIONS="--max-old-space-size=8192"
export BUILD_FAST=true
export DOCUSAURUS_URL="http://localhost:3000/"

./enterprise-temp/docs/enterprise/docusaurus/scripts/move-files.sh

cd docusaurus
yarn install --frozen-lockfile
yarn clear && yarn build && yarn serve -p 3000 -h 0.0.0.0

