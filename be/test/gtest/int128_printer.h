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

#include <gtest/gtest.h>

#include "runtime/large_int_value.h"

#if !defined(__clang__)
// NOTE: (only for GCC)
// provide a template specialization to help gtest find the correct printer for `__int128`.
namespace testing {
template <>
std::string PrintToString(const __int128& value) {
    ::std::stringstream ss;
    starrocks::operator<<(ss, value);
    return ss.str();
}
} // namespace testing
#endif
