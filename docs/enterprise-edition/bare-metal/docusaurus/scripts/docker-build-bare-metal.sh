#! /bin/bash

DOCUSAURUS_DIR=`pwd`
export $(cat ${DOCUSAURUS_DIR}/.env | xargs)
echo "Using OSS: ${OSS_DIR} and"
echo "Using BYOC: ${BYOC_DIR} and"
echo "Using Bare-Metal: ${BARE_METAL_DIR}"
echo "Using PDF_DIR: ${PDF_DIR}"
echo "Using START_URL: ${START_URL}"
DOCS_DIR="$(dirname "$DOCUSAURUS_DIR")"
docker run --rm --interactive --tty \
	-e DISABLE_VERSIONING=true \
	-e START_URL=${START_URL} \
	--volume $DOCUSAURUS_DIR/:/app/temp/config:ro \
	--volume ${OSS_DIR}/docs/en:/app/temp/oss-temp:ro \
	--volume ${BYOC_DIR}:/app/temp/byoc-temp:ro \
	--volume ${BARE_METAL_DIR}:/app/temp/bare-metal-temp:ro \
	--volume ${PDF_DIR}:/app/temp/PDF_DIR:rw \
	-p 3000:3000 \
	bare-metal-build /app/temp/config/scripts/build-bare-metal.sh
	#-p 3000:3000 \
	#docs-build yarn start -p 3000 -h 0.0.0.0
