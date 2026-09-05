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

# This file compile all protobuf files.

BUILD_DIR = ${CURDIR}/../build/

ifneq ("$(wildcard ${STARROCKS_THIRDPARTY}/installed/bin/protoc)","")
	PROTOC = ${STARROCKS_THIRDPARTY}/installed/bin/protoc
else
	PROTOC = protoc
endif

SOURCES = $(shell find ${CURDIR} -name "*.proto")
STAMPS = $(patsubst ${CURDIR}/%.proto, ${BUILD_DIR}/gen_cpp/.%_proto.stamp, ${SOURCES})

#all: ${JAVA_OBJECTS} ${OBJECTS} ${HEADERS}
all: ${STAMPS}
.PHONY: all

FORCE:
.PHONY: FORCE

# Use a content hash stamp to avoid regenerating when only timestamps change.
# Use sha256sum if available, otherwise fall back to shasum (macOS).
# If generated outputs were removed, force regeneration even with a matching hash.
${BUILD_DIR}/gen_cpp/.%_proto.stamp: ${CURDIR}/%.proto FORCE | ${BUILD_DIR}/gen_cpp
	@set -e; \
	checksum="$$( (command -v sha256sum >/dev/null 2>&1 && sha256sum "$<" || shasum -a 256 "$<") | awk '{print $$1}')"; \
	if [ -f "$@" ] && [ "$$(cat "$@")" = "$$checksum" ]; then \
		base="$$(basename "$<" .proto)"; \
		if [ -f "${BUILD_DIR}/gen_cpp/$${base}.pb.h" ] && [ -f "${BUILD_DIR}/gen_cpp/$${base}.pb.cc" ]; then \
			touch "$@"; \
			exit 0; \
		fi; \
	fi; \
	echo "Generating proto $<"; \
	${PROTOC} --proto_path=${CURDIR} --cpp_out=${BUILD_DIR}/gen_cpp "$<"; \
	echo "$$checksum" > "$@"

${BUILD_DIR}/gen_cpp/%.pb.h ${BUILD_DIR}/gen_cpp/%.pb.cc: ${BUILD_DIR}/gen_cpp/.%_proto.stamp ;

${BUILD_DIR}/gen_cpp:
	mkdir -p $@

#${BUILD_DIR}/java:
#	mkdir -p $@
