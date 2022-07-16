// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifdef USE_STAROS

#pragma once

#include <fs/fs.h>

namespace starrocks {

static const char* const kStarletPrefix = "staros://";

std::unique_ptr<FileSystem> new_fs_starlet();

} // namespace starrocks

#endif // USE_STAROS
