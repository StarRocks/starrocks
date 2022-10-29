// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <assert.h> // NOLINT(modernize-deprecated-headers)
#include <fmt/format.h>

#include <string_view>

namespace starrocks::lake {

inline std::string join_path(std::string_view parent, std::string_view child) {
    assert(!parent.empty());
    assert(!child.empty());
    assert(child.back() != '/');
    if (parent.back() != '/') {
        return fmt::format("{}/{}", parent, child);
    }
    return fmt::format("{}{}", parent, child);
}

} // namespace starrocks::lake
