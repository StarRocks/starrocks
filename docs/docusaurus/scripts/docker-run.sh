#! /bin/bash
DOCUSAURUS_DIR=`pwd`
DOCS_DIR="$(dirname "$DOCUSAURUS_DIR")"
docker run --rm --interactive --tty \
	-e DISABLE_VERSIONING=true \
	--volume $DOCS_DIR/en:/app/docusaurus/docs \
	--volume "$DOCS_DIR/zh:/app/docusaurus/i18n/zh/docusaurus-plugin-content-docs/current" \
	-p 3000:3000 \
	docs-build yarn start -p 3000 -h 0.0.0.0
