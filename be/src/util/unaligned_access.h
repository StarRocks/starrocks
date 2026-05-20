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

#include <cstdint>
#include <type_traits>

<<<<<<< HEAD:be/src/util/unaligned_access.h
namespace starrocks {
=======
namespace starrocks::config {
// Enable cow optimization for column operations, used to avoid the overhead of reference counting when accessing
// columns.
CONF_mBool(enable_cow_optimization, "false");
>>>>>>> 8dbc74b70e ([BugFix] Disable COW optimization due to design flaws causing crashes (#73480)):be/src/common/config_cow_fwd.h

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
