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
  bthreads/future_impl.cpp
  concurrency/await.cpp
  concurrency/core_local.cpp
  concurrency/spinlock.cpp
  container/lru_cache.cpp
  crypto/aes_util.cpp
  crypto/md5.cpp
  crypto/sha.cpp
  crypto/sm3.cpp
  debug/debug_action.cpp
  debug/trace.cpp
  debug/trace_metrics.cpp
  coding.cpp
  gc/gc_helper.cpp
  network/network_util.cpp
  network/cidr.cpp
  gc/gc_helper_smoothstep.cpp
  hash/crc32c.cpp
  hash/crc32c_sse_simd.cpp
  hash/hash_util.cpp
  hash/murmur_hash3.cpp
  failpoint/fail_point.cpp
  path/filesystem_util.cc
  process/lite_exec.cpp
  types/decimal12.cpp
  types/int128.cpp
  types/int256.cpp
  types/int96.cpp
  json/easy_json.cc
  path/file_util.cpp
  path/path_builder.cpp
  path/path_util.cpp
  random/random.cc
  simd/expand.cpp
  stats/ndv_estimator.cpp
  metrics.cpp
  string/base85.cpp
  string/parse_util.cpp
  string/faststring.cc
  string/string_util.cpp
  string/string_parser.cpp
  string/url_parser.cpp
  string/slice.cpp
  string/utf8_check.cpp
  system/errno.cpp
  time/date_func.cpp
  utility/arrow_utils.cpp
  utility/dynamic_util.cpp
  time/monotime.cpp
  time/time.cpp
  time/timezone_utils.cpp
  template/mustache/mustache.cc
  testutil/assert.cpp
  testutil/sync_point.cc
  testutil/sync_point_impl.cc
  uid_util.cpp
  url_coding.cpp
  uuid/uuid_generator.cpp
  status.cpp
  statusor.cpp
)
