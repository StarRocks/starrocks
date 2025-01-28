#! /bin/bash

DOCUSAURUS_DIR=`pwd`
DOCS_DIR="$(dirname "$DOCUSAURUS_DIR")"
docker run --rm --interactive --tty \
	-e DISABLE_VERSIONING=true \
	--volume $DOCS_DIR:/app/docusaurus/docs:ro \
	-p 3000:3000 \
	ee-docs-build /app/docusaurus/scripts/ee-build-and-serve.sh
	#ee-docs-build bash
