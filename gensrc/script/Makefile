# This file is made available under Elastic License 2.0
# This file is based on code available under the Apache license here:
#   https://github.com/apache/incubator-doris/blob/master/gensrc/script/Makefile

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

# generate files with python

BUILD_DIR = ${CURDIR}/../build/
FE_TARGET_DIR = ${CURDIR}/../../fe/fe-core/target/generated-sources/build

# Prerequisites on the right side of '|' is only order
all: gen_version gen_vectorized_functions
.PHONY: all

${BUILD_DIR}/python:
	mkdir -p $@

${BUILD_DIR}/python/vectorized:
	mkdir -p $@

# generated vectorized engine function
GEN_VECTORIZED_FUNCTIONS_OUTPUT = ${FE_TARGET_DIR}/com/starrocks/builtins/VectorizedBuiltinFunctions.java  \
                                  ${BUILD_DIR}/gen_cpp/opcode/builtin_functions.cpp

${GEN_VECTORIZED_FUNCTIONS_OUTPUT}: vectorized/vectorized_functions.py vectorized/gen_vectorized_functions.py | ${BUILD_DIR}/python/vectorized
	cd ${BUILD_DIR}/python/vectorized && ${PYTHON} ${CURDIR}/vectorized/gen_vectorized_functions.py

gen_vectorized_functions: ${GEN_VECTORIZED_FUNCTIONS_OUTPUT}
.PHONY: gen_vectorized_functions

# generate version info
gen_version:
	${PYTHON} ${CURDIR}/../../build-support/gen_build_version.py --cpp ${BUILD_DIR}/gen_cpp --java ${FE_TARGET_DIR}
.PHONY: gen_version


