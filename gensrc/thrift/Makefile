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
#
# This file is based on code available under the Apache license here:
#   https://github.com/apache/incubator-doris/blob/master/gensrc/thrift/Makefile

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This file used to compile all thrift files.
BUILD_DIR = ${CURDIR}/../build/

ifneq ("$(wildcard ${STARROCKS_THIRDPARTY}/installed/bin/thrift)","")
	THRIFT = ${STARROCKS_THIRDPARTY}/installed/bin/thrift
else
	THRIFT = thrift
endif

SOURCES = $(shell find ${CURDIR} -name "*.thrift")
OBJECTS = $(patsubst ${CURDIR}/%.thrift, ${BUILD_DIR}/gen_cpp/%_types.cpp, ${SOURCES})

GEN_SOURCES = $(shell find ${CURDIR} -name "*.thrift")
GEN_OBJECTS = $(patsubst ${BUILD_DIR}/thrift/%.thrift, ${BUILD_DIR}/gen_cpp/%_types.cpp, ${GEN_SOURCES})

all: ${GEN_OBJECTS} ${OBJECTS}
.PHONY: all

THRIFT_CPP_ARGS = -I ${CURDIR} -I ${BUILD_DIR}/thrift/ --allow-64bit-consts --gen cpp -out ${BUILD_DIR}/gen_cpp

${BUILD_DIR}/gen_cpp:
	mkdir -p $@

# handwrite thrift
${BUILD_DIR}/gen_cpp/%_types.cpp: ${CURDIR}/%.thrift | ${BUILD_DIR}/gen_cpp
	${THRIFT} ${THRIFT_CPP_ARGS} $<

# The default generated header file contains too many unnecessary includes, this will leads a
# slow compilation speed. We patch a diff to remove these includes.
${BUILD_DIR}/gen_cpp/StatusCode_types.cpp: ${CURDIR}/StatusCode.thrift | ${BUILD_DIR}/gen_cpp
	${THRIFT} ${THRIFT_CPP_ARGS} $<
	patch -p0 -d ${BUILD_DIR}/gen_cpp < StatusCode_types.diff 2>&1 1>/dev/null
