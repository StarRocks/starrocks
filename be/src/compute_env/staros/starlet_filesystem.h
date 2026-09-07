// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#ifdef USE_STAROS

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "common/statusor.h"
#include "fs/fs.h"

namespace starrocks {

// is_starlet_uri() performs less strict verification than parse_starlet_uri(), which means
// if is_starlet_uri() returns false parse_starlet_uri() must fail and if is_starlet_uri()
// returns true parse_starlet_uri() may also fail.
bool is_starlet_uri(std::string_view uri);

std::string build_starlet_uri(int64_t shard_id, std::string_view path);

// The first element of pair is path, the second element of pair is shard id.
//      staros://shardid/over/there
//      \__/    \_____/ \_______/
//       |         |        |
//     scheme   shard_id   path
//
// If parse_starlet_uri() succeeded, is_starlet_uri() must be true.
StatusOr<std::pair<std::string, int64_t>> parse_starlet_uri(std::string_view uri);

std::unique_ptr<FileSystem> new_fs_starlet();

#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
namespace fs {

struct FileSystemProvider;

FileSystemProvider new_starlet_file_system_provider(int priority = 30);

} // namespace fs
#endif // defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)

// Create a starlet filesystem for cross-cluster migration with optional S3 raw path mode.
//
// When `use_raw_path` is true:
//   - Sets s3.use_raw_path_with_scheme=true in starlet configuration
//   - Starlet will use the input path as-is without normalize_path processing
//   - This is required for S3 storage type to support partitioned prefix feature
//
// When `use_raw_path` is false:
//   - Uses default starlet configuration
//   - Starlet will use normalize_path to combine sys.root with the relative path
//   - This is used for non-S3 storage types (OSS/Azure/HDFS/GFS)
std::shared_ptr<FileSystem> new_fs_starlet(int64_t virtual_shard_id, bool use_raw_path);

#ifdef BE_TEST
// Drop every entry from the process-wide shard filesystem cache that new_fs_starlet(shard_id, ...)
// consults before it creates anything.
//
// That cache is keyed by shard id only and lives for the life of the process, so a filesystem a test
// injected through the "new_fs_starlet::get_shard_filesystem" sync point stays reachable by every
// later test using the same shard id -- and because a cache HIT returns early, the later test's own
// sync-point callback is never invoked. The next test then silently runs against its predecessor's
// mock: one expecting filesystem creation to fail instead gets a working mock, and one reading file
// content gets the previous test's bytes. Fixtures that inject a filesystem must clear the cache so
// they neither inherit nor leak one.
void TEST_clear_shard_fs_cache();
#endif

} // namespace starrocks

#endif // USE_STAROS
