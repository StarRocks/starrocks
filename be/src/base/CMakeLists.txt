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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/base")

ADD_BE_LIB(Base
  concurrency/await.cpp
  concurrency/spinlock.cpp
  crypto/md5.cpp
  crypto/sha.cpp
  crypto/sm3.cpp
  gc/gc_helper_smoothstep.cpp
  hash/crc32c_sse_simd.cpp
  hash/murmur_hash3.cpp
  json/easy_json.cc
  path/file_util.cpp
  path/path_builder.cpp
  path/path_util.cpp
  random/random.cc
  simd/expand.cpp
  stats/ndv_estimator.cpp
  string/faststring.cc
  string/slice.cpp
  string/utf8_check.cpp
  system/errno.cpp
  template/mustache/mustache.cc
  testutil/assert.cpp
  testutil/sync_point.cc
  testutil/sync_point_impl.cc
  uuid/uuid_generator.cpp
  status.cpp
  statusor.cpp
)
