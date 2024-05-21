#! /bin/bash

DOCUSAURUS_DIR=`pwd`
DOCS_DIR="$(dirname "$DOCUSAURUS_DIR")"
docker run --rm --interactive --tty \
	-e DISABLE_VERSIONING=true \
	--volume $DOCS_DIR/docusaurus/ee-sidebars.json:/app/docusaurus/sidebars.json \
	--volume $DOCS_DIR/en:/app/docusaurus/docs-temp \
	--volume $DOCS_DIR/enterprise-edition/en:/app/docusaurus/ee-docs \
	--volume "$DOCS_DIR/zh:/app/docusaurus/i18n/zh/docusaurus-plugin-content-docs/current" \
	-p 3000:3000 \
	ee-docs-build /app/docusaurus/scripts/ee-build-and-serve.sh
	#ee-docs-build bash
