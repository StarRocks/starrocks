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

#include <util/stack_util.h>

#include <cstdint>
#include <functional>

namespace starrocks {

// take a sleep with small intervals until time out by `sleep_secs` or the `stop_condition()` is true
void nap_sleep(int32_t sleep_secs, const std::function<bool()>& stop_condition);

#if defined(__GNUC__) || defined(__clang__)
#define NOT_SUPPORT()                                                                        \
    do {                                                                                     \
        throw std::runtime_error(std::string("Not support method '") + __PRETTY_FUNCTION__ + \
                                 "': " + get_stack_trace());                                 \
    } while (0);
#elif defined(_MSC_VER)
#define NOT_SUPPORT()                                                                                            \
    do {                                                                                                         \
        throw std::runtime_error(std::string("Not support method '") + __FUNCSIG__ + "': " + get_stack_trace()); \
    } while (0);
#else
#define NOT_SUPPORT()                                                                                         \
    do {                                                                                                      \
        throw std::runtime_error(std::string("Not support method '") + __func__ + "': " + get_stack_trace()); \
    } while (0);
#endif
} // namespace starrocks
