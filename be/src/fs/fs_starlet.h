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

} // namespace starrocks

#endif // USE_STAROS
