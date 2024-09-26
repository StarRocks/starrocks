#!/bin/bash
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#####################################################
# Download url, filename and unpaced filename
# of all thirdparties
# 
# vars-${arch}.sh defines the thirdparties that are
# architecure-related.
#####################################################

# cachelib
CACHELIB_DOWNLOAD="https://cdn-thirdparty.starrocks.com/cachelib/cachelib-ubuntu22-20240926.tar.gz"
CACHELIB_NAME="cachelib.tar.gz"
CACHELIB_SOURCE="cachelib"
CACHELIB_MD5SUM="a5974c4f198c105f08f40adeb6ec940f"
