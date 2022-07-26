// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifdef USE_STAROS

#pragma once

#include "common/statusor.h"
#include "fs/fs.h"

namespace starrocks {

std::string build_starlet_uri(int64_t shard_id, std::string_view path);

// The first element of pair is path, the second element of pair is shard id.
StatusOr<std::pair<std::string_view, int64_t>> parse_starlet_uri(std::string_view uri);

std::unique_ptr<FileSystem> new_fs_starlet();

} // namespace starrocks

#endif // USE_STAROS
