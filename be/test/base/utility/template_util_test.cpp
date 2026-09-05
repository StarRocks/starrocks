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

#include "base/utility/template_util.h"

#include <gtest/gtest.h>

#include <string>
#include <type_traits>

namespace starrocks {
namespace {

struct FloatOverloadTester {
    template <typename T>
    ENABLE_IF_FLOAT(T, int)
    foo(T) {
        return 1;
    }

    template <typename T>
    ENABLE_IF_INTEGRAL(T, int)
    foo(T) {
        return 2;
    }
};

template <typename T, typename = void>
struct has_foo : std::false_type {};

template <typename T>
struct has_foo<T, std::void_t<decltype(std::declval<FloatOverloadTester>().foo(std::declval<T>()))>> : std::true_type {
};

} // namespace

TEST(TemplateUtilTest, EnableIfFloatRejectsNonFloatingTypes) {
    EXPECT_TRUE((has_foo<float>::value));
    EXPECT_TRUE((has_foo<double>::value));
    EXPECT_TRUE((has_foo<int>::value));
    EXPECT_FALSE((has_foo<std::string>::value));
}

} // namespace starrocks
