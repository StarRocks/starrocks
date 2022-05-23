// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>

#include "fs.h"

namespace starrocks {

std::unique_ptr<FileSystem> new_fs_posix();

}
