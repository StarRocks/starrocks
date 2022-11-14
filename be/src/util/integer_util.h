// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <fmt/format.h>

namespace starrocks {
template <class T>
inline std::string integer_to_string(T value) {
    char buf[64] = {0};
    auto end = fmt::format_to(buf, "{}", value);
    int len = end - buf;
    return std::string(buf, len);
}
} // namespace starrocks
