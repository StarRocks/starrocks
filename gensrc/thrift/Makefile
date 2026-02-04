# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file used to compile all thrift files.
BUILD_DIR = ${CURDIR}/../build/

ifneq ("$(wildcard ${STARROCKS_THIRDPARTY}/installed/bin/thrift)","")
	THRIFT = ${STARROCKS_THIRDPARTY}/installed/bin/thrift
else
	THRIFT = thrift
endif

SOURCES = $(shell find ${CURDIR} -name "*.thrift")
STAMPS = $(patsubst ${CURDIR}/%.thrift, ${BUILD_DIR}/gen_cpp/.%_thrift.stamp, ${SOURCES})

all: ${STAMPS}
.PHONY: all

FORCE:
.PHONY: FORCE

THRIFT_CPP_ARGS = -I ${CURDIR} -I ${BUILD_DIR}/thrift/ --allow-64bit-consts --gen cpp -out ${BUILD_DIR}/gen_cpp

${BUILD_DIR}/gen_cpp:
	mkdir -p $@

# Only regenerate when the thrift content changes, not when timestamps differ.
# We record a content hash in a stamp file and compare it on subsequent runs.
# Use sha256sum if available, otherwise fall back to shasum (macOS).
# If generated outputs were removed, force regeneration even with a matching hash.
define THRIFT_STAMP_RECIPE
@set -e; \
checksum="$$( (command -v sha256sum >/dev/null 2>&1 && sha256sum "$<" || shasum -a 256 "$<") | awk '{print $$1}')"; \
if [ -f "$@" ] && [ "$$(cat "$@")" = "$$checksum" ]; then \
	missing=0; \
	check_file() { [ -f "$$1" ] || missing=1; }; \
	base="$$(basename "$<" .thrift)"; \
	check_file "${BUILD_DIR}/gen_cpp/$${base}_types.h"; \
	check_file "${BUILD_DIR}/gen_cpp/$${base}_types.cpp"; \
	if grep -Eq '^[[:space:]]*const[[:space:]]' "$<"; then \
		check_file "${BUILD_DIR}/gen_cpp/$${base}_constants.h"; \
		check_file "${BUILD_DIR}/gen_cpp/$${base}_constants.cpp"; \
	fi; \
	services="$$(awk '/^[[:space:]]*service[[:space:]]+[A-Za-z_][A-Za-z0-9_]*/ {print $$2}' "$<")"; \
	for svc in $$services; do \
		check_file "${BUILD_DIR}/gen_cpp/$${svc}.h"; \
		check_file "${BUILD_DIR}/gen_cpp/$${svc}.cpp"; \
		check_file "${BUILD_DIR}/gen_cpp/$${svc}_server.skeleton.cpp"; \
	done; \
	if [ "$$missing" -eq 0 ]; then \
		touch "$@"; \
		exit 0; \
	fi; \
fi; \
echo "Generating thrift $<"; \
${THRIFT} ${THRIFT_CPP_ARGS} "$<"; \
$(if $(strip $(1)),$(1),:) ; \
echo "$$checksum" > "$@"
endef

${BUILD_DIR}/gen_cpp/.%_thrift.stamp: ${CURDIR}/%.thrift FORCE | ${BUILD_DIR}/gen_cpp
	$(call THRIFT_STAMP_RECIPE,)

# The default generated header file contains too many unnecessary includes, this will leads a
# slow compilation speed. We patch a diff to remove these includes.
# Use sha256sum if available, otherwise fall back to shasum (macOS).
# If generated outputs were removed, force regeneration even with a matching hash.
${BUILD_DIR}/gen_cpp/.StatusCode_thrift.stamp: ${CURDIR}/StatusCode.thrift FORCE | ${BUILD_DIR}/gen_cpp
	$(call THRIFT_STAMP_RECIPE,if grep -q "TApplicationException" "${BUILD_DIR}/gen_cpp/StatusCode_types.h"; then \
		patch -p0 -d ${BUILD_DIR}/gen_cpp < StatusCode_types.diff 2>&1 1>/dev/null; \
	fi)
