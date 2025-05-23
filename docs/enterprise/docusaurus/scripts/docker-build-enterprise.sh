#! /bin/bash

ENTERPRISE_DIR=`pwd`

docker run --rm --interactive --tty \
	-e DISABLE_VERSIONING=true \
	--volume ${ENTERPRISE_DIR}:/app/enterprise-temp:ro \
	-p 3000:3000 \
	enterprise-build bash -c ./enterprise-temp/docs/enterprise/docusaurus/scripts/build-enterprise.sh
	#enterprise-build bash -c "echo 'run:' && echo './enterprise-temp/docs/enterprise/docusaurus/scripts/build-enterprise.sh' && bash"

	#bare-metal-build /app/temp/config/scripts/build-bare-metal.sh
	#-p 3000:3000 \
	#docs-build yarn start -p 3000 -h 0.0.0.0
