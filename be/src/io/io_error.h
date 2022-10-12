// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <fmt/format.h>

#include <cerrno>
#include <string>

#include "common/status.h"

namespace starrocks::io {

inline Status io_error(const std::string& context, int err_number) {
    switch (err_number) {
    case ENOENT:
        return Status::NotFound(fmt::format("{}: {}", context, std::strerror(err_number)));
    case EEXIST:
        return Status::AlreadyExist(fmt::format("{}: {}", context, std::strerror(err_number)));
    default:
        return Status::IOError(fmt::format("{}: {}", context, std::strerror(err_number)));
    }
}

} // namespace starrocks::io
