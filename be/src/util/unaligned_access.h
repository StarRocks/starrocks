// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstdint>
#include <type_traits>

namespace starrocks {

template <typename T>
inline T unaligned_load(const void* p) {
    T res{};
    memcpy(&res, p, sizeof(res));
    return res;
}

// Using `std::common_type` to disable argument-based template parameter deduction.
template <typename T>
inline void unaligned_store(void* p, typename std::common_type<T>::type val) {
    static_assert(std::is_trivially_copyable_v<T>);
    memcpy(p, &val, sizeof(val));
}

} // namespace starrocks
