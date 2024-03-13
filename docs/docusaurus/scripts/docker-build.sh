#! /bin/bash

DOCUSAURUS_DIR=`pwd`
DOCS_DIR="$(dirname "$DOCUSAURUS_DIR")"
docker run --rm --interactive --tty \
	-e DISABLE_VERSIONING=true \
	--volume $DOCS_DIR/docusaurus/sidebars.json:/app/docusaurus/sidebars.json \
	--volume $DOCS_DIR/en:/app/docusaurus/docs \
	--volume "$DOCS_DIR/zh:/app/docusaurus/i18n/zh/docusaurus-plugin-content-docs/current" \
	-p 3000:3000 \
	docs-build /app/docusaurus/scripts/build-and-serve.sh
