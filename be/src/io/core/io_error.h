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
