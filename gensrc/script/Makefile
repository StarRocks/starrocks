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
#   https://github.com/apache/incubator-doris/blob/master/gensrc/script/Makefile

# generate files with python

BUILD_DIR = ${CURDIR}/../build/
FE_TARGET_DIR = ${CURDIR}/../../fe/fe-core/target/generated-sources/build

# Prerequisites on the right side of '|' is only order
all: gen_version gen_functions
.PHONY: all

# generated vectorized engine function
GEN_FUNCTIONS_OUTPUT = ${FE_TARGET_DIR}/com/starrocks/builtins/VectorizedBuiltinFunctions.java  \
                                  ${BUILD_DIR}/gen_cpp/opcode/builtin_functions.cpp

${GEN_FUNCTIONS_OUTPUT}: functions.py gen_functions.py 
	${PYTHON} ${CURDIR}/gen_functions.py --cpp ${BUILD_DIR}/gen_cpp  --java ${FE_TARGET_DIR}

gen_functions: ${GEN_FUNCTIONS_OUTPUT}
.PHONY: gen_functions

# generate version info
gen_version:
	${PYTHON} ${CURDIR}/../../build-support/gen_build_version.py --cpp ${BUILD_DIR}/gen_cpp --java ${FE_TARGET_DIR}
.PHONY: gen_version


