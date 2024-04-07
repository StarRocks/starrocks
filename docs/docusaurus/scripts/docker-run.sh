#! /bin/bash

echo -n "Docusaurus builds only one locale in dev mode, please enter a locale (zh or en): "
read locale

DOCUSAURUS_DIR=`pwd`
DOCS_DIR="$(dirname "$DOCUSAURUS_DIR")"
docker run --rm --interactive --tty \
	-e DISABLE_VERSIONING=true \
	--volume $DOCS_DIR/docusaurus/sidebars.json:/app/docusaurus/sidebars.json \
	--volume $DOCS_DIR/en:/app/docusaurus/docs \
	--volume "$DOCS_DIR/zh:/app/docusaurus/i18n/zh/docusaurus-plugin-content-docs/current" \
	-p 3000:3000 \
	docs-build yarn start -p 3000 -h 0.0.0.0 --locale $locale
